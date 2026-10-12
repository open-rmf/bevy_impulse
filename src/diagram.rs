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
mod diagram_context;
mod fork_clone_schema;
mod fork_result_schema;
mod inference;
mod join_schema;
mod node_schema;
mod operation_ref;
mod output_ref;
mod registration;
mod scope_schema;
mod script_schema;
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

#[cfg(feature = "python")]
pub mod process_bound_python;

use bevy_derive::{Deref, DerefMut};
use bevy_ecs::system::Commands;
pub use buffer_schema::*;
pub use diagram_context::*;
pub use fork_clone_schema::{DynForkClone, ForkCloneSchema, RegisterClone};
pub use fork_result_schema::{DynForkResult, ForkResultSchema};
pub use inference::*;
pub use join_schema::{JoinRegistration, JoinSchema};
pub use node_schema::NodeSchema;
pub use operation_ref::*;
pub use output_ref::*;
pub use registration::*;
pub use scope_schema::*;
pub use script_schema::*;
pub use section_schema::*;
pub use serialization::*;
pub use split_schema::*;
pub use stream_out_schema::*;
use tracing::debug;
pub use transform_schema::{TransformError, TransformSchema};
pub use unzip_schema::UnzipSchema;
pub use workflow_builder::*;

use anyhow::Error as Anyhow;

use std::{borrow::Cow, collections::HashMap, fmt::Display, io::Read, sync::Arc};

pub use crate::type_info::TypeInfo;
use crate::{
    Builder, DuplicateBuffer, IdentifierRef, IncompatibleLayout, IncrementalScopeError,
    JsonMessage, MessageTypeHint, Scope, Service, SpawnWorkflowExt, SplitConnectionError,
    StreamPack, TryJoinError, format_list, is_default,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Visitor},
    ser::SerializeMap,
};

use thiserror::Error as ThisError;

const CURRENT_DIAGRAM_VERSION: &str = "0.1.0";
const SUPPORTED_DIAGRAM_VERSION: &str = ">=0.1.0, <0.2.0";
const RESERVED_OPERATION_NAMES: [&'static str; 2] = ["", "builtin"];

pub type BuilderId = Arc<str>;
pub type OperationName = Arc<str>;
pub type Text = Arc<str>;

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

    pub fn in_namespace(&self, namespace: &OperationName) -> OperationRef {
        let op: OperationRef = self.into();
        op.in_namespaces(&[Arc::clone(namespace)])
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
pub enum BufferSelection<Selection = NextOperation> {
    Dict(HashMap<String, Selection>),
    Array(Vec<Selection>),
}

impl<Selection> BufferSelection<Selection> {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Dict(d) => d.is_empty(),
            Self::Array(a) => a.is_empty(),
        }
    }

    pub fn iter(&self) -> BufferSelectionIter<'_, Selection> {
        match self {
            Self::Dict(dict) => BufferSelectionIter::Dict(dict.iter()),
            Self::Array(array) => BufferSelectionIter::Array(array.iter().enumerate()),
        }
    }
}

pub enum BufferSelectionIter<'a, Selection> {
    Dict(std::collections::hash_map::Iter<'a, String, Selection>),
    Array(std::iter::Enumerate<std::slice::Iter<'a, Selection>>),
}

impl<'a, Selection> Iterator for BufferSelectionIter<'a, Selection> {
    type Item = (IdentifierRef<'a>, &'a Selection);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dict(dict) => dict
                .next()
                .map(|(name, id)| (IdentifierRef::Name(Cow::Borrowed(name.as_str())), id)),
            Self::Array(array) => array
                .next()
                .map(|(index, id)| (IdentifierRef::Index(index), id)),
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
    ///
    /// When there is a path within a scope that can reach a cancel operation,
    /// we consider that scope to still be reachable. This allows you to safely
    /// and reliably perform cleanup actions along the path to a cancellation.
    /// However this also means that if any part of a scope can reach a cancel
    /// operation then the scope will keep running, even if that part of the
    /// workflow far removed from the intended happy path of the workflow.
    /// Consider using implicit cancel if you do not want the cancellation to
    /// affect the lifespan of the scope.
    Cancel,

    /// Same as cancel, except using this will not affect the lifespan of the
    /// scope.
    ImplicitCancel,
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
    Transform(TransformSchema),
    Buffer(BufferSchema),
    BufferAccess(BufferAccessSchema),
    Listen(ListenSchema),
    Script(ScriptSchema),
}

impl BuildDiagramOperation for DiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut BuilderContext,
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
            Self::Script(op) => op.build_diagram_operation(id, ctx),
            Self::Section(op) => op.build_diagram_operation(id, ctx),
            Self::Split(op) => op.build_diagram_operation(id, ctx),
            Self::StreamOut(op) => op.build_diagram_operation(id, ctx),
            Self::Transform(op) => op.build_diagram_operation(id, ctx),
            Self::Unzip(op) => op.build_diagram_operation(id, ctx),
        }
    }

    fn apply_message_type_constraints(
        &self,
        id: &OperationName,
        ctx: &mut InferenceContext,
    ) -> Result<(), DiagramErrorCode> {
        match self {
            Self::Buffer(op) => op.apply_message_type_constraints(id, ctx),
            Self::BufferAccess(op) => op.apply_message_type_constraints(id, ctx),
            Self::ForkClone(op) => op.apply_message_type_constraints(id, ctx),
            Self::ForkResult(op) => op.apply_message_type_constraints(id, ctx),
            Self::Join(op) => op.apply_message_type_constraints(id, ctx),
            Self::Listen(op) => op.apply_message_type_constraints(id, ctx),
            Self::Node(op) => op.apply_message_type_constraints(id, ctx),
            Self::Scope(op) => op.apply_message_type_constraints(id, ctx),
            Self::Script(op) => op.apply_message_type_constraints(id, ctx),
            Self::Section(op) => op.apply_message_type_constraints(id, ctx),
            Self::Split(op) => op.apply_message_type_constraints(id, ctx),
            Self::StreamOut(op) => op.apply_message_type_constraints(id, ctx),
            Self::Transform(op) => op.apply_message_type_constraints(id, ctx),
            Self::Unzip(op) => op.apply_message_type_constraints(id, ctx),
        }
    }

    fn child_operations(
        &self,
        templates: &Templates,
    ) -> Result<Option<Operations>, DiagramErrorCode> {
        match self {
            Self::Buffer(op) => op.child_operations(templates),
            Self::BufferAccess(op) => op.child_operations(templates),
            Self::ForkClone(op) => op.child_operations(templates),
            Self::ForkResult(op) => op.child_operations(templates),
            Self::Join(op) => op.child_operations(templates),
            Self::Listen(op) => op.child_operations(templates),
            Self::Node(op) => op.child_operations(templates),
            Self::Scope(op) => op.child_operations(templates),
            Self::Script(op) => op.child_operations(templates),
            Self::Section(op) => op.child_operations(templates),
            Self::Split(op) => op.child_operations(templates),
            Self::StreamOut(op) => op.child_operations(templates),
            Self::Transform(op) => op.child_operations(templates),
            Self::Unzip(op) => op.child_operations(templates),
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

#[derive(Default, Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ExtensionSettings {
    /// Settings for each extension.
    #[serde(default)]
    pub extensions: HashMap<String, serde_json::Value>,
}

#[derive(Default, Debug, Clone, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InputExample {
    pub value: JsonMessage,
    pub description: String,
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

    /// Section templates used by this diagram.
    #[serde(default)]
    pub templates: Templates,

    /// Custom script environments used by this diagram.
    ///
    /// To run a script operation you will need to specify what its environment
    /// is. A script environment determines the language and interpreter for the
    /// script, as well as any other factors specific to the environment.
    ///
    /// Script environment builders may have automatic configs, which you should
    /// consider using before creating a custom environment.
    #[serde(default)]
    pub script_environments: HashMap<OperationName, ScriptEnvironmentSchema>,

    /// Indicates where the workflow should start running.
    pub start: NextOperation,

    /// To simplify diagram definitions, the diagram workflow builder will
    /// sometimes insert implicit operations into the workflow, such as implicit
    /// serializing and deserializing. These implicit operations may be fallible.
    ///
    /// This field indicates how a failed implicit operation should be handled.
    /// If left unspecified, an implicit error will cause the entire workflow to
    /// be cancelled.
    #[serde(default, skip_serializing_if = "is_default")]
    pub on_implicit_error: Option<NextOperation>,

    /// Operations that define the workflow
    pub ops: Operations,

    /// Whether the operations in the workflow should be traced by default.
    /// Being traced means each operation will emit an event each time it is
    /// triggered. You can decide whether that event contains the serialized
    /// message data that triggered the operation.
    ///
    /// If crossflow is not compiled with the "trace" feature then this
    /// setting will have no effect.
    #[serde(default, skip_serializing_if = "is_default")]
    pub default_trace: TraceToggle,

    #[serde(flatten)]
    pub extensions: Option<ExtensionSettings>,

    /// Optional text to describe the workflow.
    #[serde(default, skip_serializing_if = "is_default")]
    pub description: String,

    /// Examples of inputs that can be used with this workflow.
    #[serde(default, skip_serializing_if = "is_default")]
    input_examples: Vec<InputExample>,
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
    pub display_text: Option<Text>,
    /// Set what the tracing behavior should be for this operation. If this is
    /// left unspecified then the default trace setting of the diagram will be
    /// used.
    #[serde(default, skip_serializing_if = "is_default")]
    pub trace: Option<TraceToggle>,

    #[serde(flatten)]
    pub extensions: Option<ExtensionSettings>,
}

impl Diagram {
    /// Begin creating a new diagram
    pub fn new(start: NextOperation) -> Self {
        Self {
            version: semver::Version::parse(CURRENT_DIAGRAM_VERSION).unwrap(),
            start,
            script_environments: Default::default(),
            templates: Default::default(),
            on_implicit_error: Default::default(),
            ops: Default::default(),
            default_trace: Default::default(),
            extensions: None,
            description: Default::default(),
            input_examples: Default::default(),
        }
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossflow::*;
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = DiagramElementRegistry::new();
    /// registry.register_node_builder(NodeBuilderOptions::new("echo".to_string()), |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| {
    ///         println!("{msg}");
    ///         msg
    ///     })
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
    pub fn spawn_workflow<Request, Response, Streams>(
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

        let w = cmds.spawn_workflow(
            |scope: Scope<Request, Response, Streams>, builder: &mut Builder| {
                debug!(
                    "spawn workflow, scope input: {:?}, terminate: {:?}",
                    scope.start.id(),
                    scope.terminate.id()
                );

                if let Err(had_err) = create_workflow(scope, builder, registry, self) {
                    err = Some(had_err);
                }
            },
        );

        if let Some(err) = err {
            // Despawn the workflow because we did not build it successfully.
            cmds.entity(w.provider()).despawn();
            return Err(err);
        }

        Ok(w)
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossflow::*;
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
        check_circular_operation_dependency(&self.ops, &self.templates)?;
        let mut all_ops = vec![self.ops.clone()];
        while let Some(ops) = all_ops.pop() {
            for op in ops.values() {
                if let Some(children) = op.child_operations(&self.templates)? {
                    all_ops.push(children);
                }
            }

            ops.validate_operation_names()?;
        }

        self.templates.validate_operation_names()?;
        Ok(())
    }

    /// Validate the templates that are being used within the `ops` section, or
    /// recursively within any templates used by the `ops` section. Any unused
    /// templates will not be validated.
    pub fn validate_template_usage(&self) -> Result<(), DiagramErrorCode> {
        let mut all_ops = vec![self.ops.clone()];
        while let Some(ops) = all_ops.pop() {
            for op in ops.values() {
                if let Some(children) = op.child_operations(&self.templates)? {
                    all_ops.push(children);
                }

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
        }

        Ok(())
    }

    /// Get how implicit errors will be handled in the root scope of this diagram.
    pub fn on_implicit_error(&self) -> NextOperation {
        self.on_implicit_error
            .clone()
            .unwrap_or(NextOperation::Builtin {
                builtin: BuiltinTarget::ImplicitCancel,
            })
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
        check_circular_operation_dependency(&self.get_template(template_id)?.ops, &self)?;
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

fn check_circular_operation_dependency(
    start_from: &Operations,
    templates: &Templates,
) -> Result<(), DiagramErrorCode> {
    let mut queue = Vec::new();
    queue.push(OperationStack::new(start_from, templates.clone()));

    while let Some(top) = queue.pop() {
        queue.extend(top.children()?);
    }

    Ok(())
}

#[derive(Clone)]
struct OperationStack {
    used: Vec<Operations>,
    names: Vec<OperationName>,
    templates: Templates,
    next: Operations,
}

impl OperationStack {
    fn new(op: &Operations, templates: Templates) -> Self {
        OperationStack {
            used: vec![op.clone()],
            names: Default::default(),
            templates,
            next: op.clone(),
        }
    }

    fn children(&self) -> Result<Vec<Self>, DiagramErrorCode> {
        let mut children = Vec::new();

        for (name, next_op) in self.next.iter() {
            if let Some(child_ops) = next_op.child_operations(&self.templates)? {
                children.push(self.child(Arc::clone(name), child_ops)?);
            }
        }

        Ok(children)
    }

    fn child(&self, name: OperationName, next: Operations) -> Result<Self, DiagramErrorCode> {
        let mut child = self.clone();
        child.used.push(child.next);
        child.names.push(name);
        if self.used.iter().any(|ops| Arc::ptr_eq(&ops.0, &next.0)) {
            return Err(DiagramErrorCode::CircularOperationDependency(child.names));
        }

        child.next = next;
        Ok(child)
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
    pub fn in_port(port_id: impl Into<PortRef>, code: DiagramErrorCode) -> Self {
        Self {
            context: DiagramErrorContext {
                port_id: Some(port_id.into()),
            },
            code,
        }
    }
}

pub trait WithContext {
    type Ok;
    fn in_port<P: Into<PortRef>, F: FnOnce() -> P>(self, f: F) -> Result<Self::Ok, DiagramError>;
}

impl<T> WithContext for Result<T, DiagramErrorCode> {
    type Ok = T;
    fn in_port<P: Into<PortRef>, F: FnOnce() -> P>(self, f: F) -> Result<T, DiagramError> {
        match self {
            Ok(ok) => Ok(ok),
            Err(err) => Err(err.in_port(f().into())),
        }
    }
}

#[derive(Default, Debug)]
pub struct DiagramErrorContext {
    port_id: Option<PortRef>,
}

impl Display for DiagramErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(op_id) = &self.port_id {
            write!(f, "in operation [{}],", op_id)?;
        }
        Ok(())
    }
}

#[derive(Clone, ThisError, Debug)]
pub enum DiagramErrorCode {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("node builder [{builder}] encountered an error: {error}")]
    NodeBuildingError {
        builder: BuilderId,
        error: Arc<Anyhow>,
    },

    #[error("section builder [{builder}] encountered an error: {error}")]
    SectionBuildingError {
        builder: BuilderId,
        error: Arc<Anyhow>,
    },

    #[error("script environment builder [{builder}] encountered an error: {error}")]
    ScriptEnvironmentBuildingError {
        builder: BuilderId,
        error: Arc<Anyhow>,
    },

    #[error("script failed to compile for environment {environment}: {error}")]
    ScriptCompileError {
        environment: BuilderId,
        error: Arc<Anyhow>,
    },

    #[error("operation [{0}] not found")]
    OperationNotFound(NextOperation),

    #[error("section template [{0}] does not exist")]
    TemplateNotFound(OperationName),

    #[error("{0}")]
    TypeMismatch(#[from] TypeMismatch),

    #[error("Failed to perform a downcast to {to}. Original type was {from:?}.")]
    InvalidDowncast {
        from: std::any::TypeId,
        to: TypeInfo,
    },

    #[error("{0}")]
    MissingStream(#[from] MissingStream),

    #[error("Operation [{0}] attempted to instantiate a duplicate of itself.")]
    DuplicateInputsCreated(OperationRef),

    #[error("Operation [{0}] attempted to instantiate a duplicate buffer.")]
    DuplicateBuffersCreated(OperationRef),

    #[error(
        "Missing a connection to start or terminate. A workflow cannot run with a valid connection to each."
    )]
    MissingStartOrTerminate,

    #[error("Operation [{0}] has no connections")]
    NoConnection(OperationRef),

    #[error("Cannot select message type. Choices: {}", format_list(.0))]
    AmbiguousMessageType(Vec<Cow<'static, str>>),

    #[error("Serialization was not registered for the target message type: {0}")]
    NotSerializable(TypeInfo),

    #[error("Deserialization was not registered for the target message type: {0}")]
    NotDeserializable(TypeInfo),

    #[error("The message type has no known conversion to a ScriptMessage: {0}")]
    NotScriptable(TypeInfo),

    #[error("Cloning was not registered for the target message type. Type: {0}")]
    NotCloneable(Cow<'static, str>),

    #[error("The target message type does not support unzipping. Type: {0}")]
    NotUnzippable(Cow<'static, str>),

    #[error("The unzipped message [{message}] does not have the requested element [{element}]")]
    InvalidUnzip {
        message: Cow<'static, str>,
        element: usize,
    },

    #[error(
        "Call .with_result() on your node to be able to fork its Result-type output. Type: {0}"
    )]
    CannotForkResult(Cow<'static, str>),

    #[error(
        "Response cannot be split. Make sure to use .with_split() when building the node. Type: {0}"
    )]
    NotSplittable(Cow<'static, str>),

    #[error(
        "Message cannot be joined. Make sure to use .with_join() when building the target node. Type: {0}"
    )]
    NotJoinable(Cow<'static, str>),

    #[error("Empty join is not allowed.")]
    EmptyJoin,

    #[error(transparent)]
    DuplicateBufferInJoin(#[from] DuplicateBuffer),

    #[error("Unknown buffer identifier [{unknown}] used for join containing {}", format_list(.available))]
    UnknownJoinField {
        unknown: IdentifierRef<'static>,
        available: Vec<IdentifierRef<'static>>,
    },

    #[error("There was an attempt to connect to an unknown operation: [{0}]")]
    UnknownOperation(OperationRef),

    #[error("There was an attempt to use an unknown section template: [{0}]")]
    UnknownTemplate(OperationName),

    #[error("There was an attempt to use an unknown script environment: [{0}]")]
    UnknownScriptEnvironment(OperationName),

    #[error("There was an attempt to use an unknown script environment builder: [{0}]")]
    UnknownScriptEnvironmentBuilder(OperationName),

    #[error("Could not find port in inference graph: [{0}]")]
    UnknownPort(PortRef),

    #[error("There is no valid message type for port [{0}]")]
    CannotInferType(PortRef),

    #[error("Unknown message type index: {index}. Limit: {limit}")]
    UnknownMessageTypeIndex { index: usize, limit: usize },

    #[error(
        "The message type for port [{0}] was never inferred. This suggests an implementation error in an operation."
    )]
    MessageTypeNotInferred(PortRef),

    #[error("There was an attempt to use an operation in an invalid way: [{0}]")]
    InvalidOperation(OperationRef),

    #[error(transparent)]
    CannotTransform(#[from] TransformError),

    #[error("box/unbox operation for the message is not registered")]
    CannotBoxOrUnbox,

    #[error("buffer access is not registered for {0}")]
    CannotAccessBuffers(Cow<'static, str>),

    #[error("listening is not registered for {0}")]
    CannotListen(Cow<'static, str>),

    #[error(transparent)]
    IncompatibleBuffers(#[from] IncompatibleLayout),

    #[error("buffer message type {source_type} does not satisfy the required type {target_type}")]
    IncompatibleBufferType {
        source_type: Cow<'static, str>,
        target_type: Cow<'static, str>,
    },

    #[error("inconsistent type hints for the buffer message: {}", format_list(&.0))]
    InconsistentBufferHints(Vec<MessageTypeHint>),

    #[error(
        "This error should not happen, it means the implementation of buffer hints is broken. Identifier of missing hint: {0}"
    )]
    BrokenBufferMessageTypeHint(IdentifierRef<'static>),

    #[error(transparent)]
    SectionError(#[from] SectionError),

    #[error("one or more operation is missing inputs")]
    IncompleteDiagram,

    #[error("the config of the operation has an error: {0}")]
    ConfigError(Arc<serde_json::Error>),

    #[error("failed to create trace info for the operation: {0}")]
    TraceInfoError(Arc<serde_json::Error>),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),

    #[error("a type being used in the diagram was not registered: {}", format_list(&.0))]
    UnregisteredTypes(Vec<Cow<'static, str>>),

    #[error("The build of the workflow came to a halt, reasons:\n{reasons:?}")]
    BuildHalted {
        /// Reasons that operations were unable to make progress building
        reasons: HashMap<OperationRef, DiagramErrorCode>,
    },

    #[error(
        "The workflow building process has had an excessive number of iterations. \
        This may indicate an implementation bug or an extraordinarily complex diagram."
    )]
    ExcessiveIterations,

    #[error("An operation was given a reserved name [{0}]")]
    InvalidUseOfReservedName(&'static str),

    #[error("an error happened while building a nested diagram: {0}")]
    NestedError(Arc<DiagramError>),

    #[error("A circular redirection exists between operations: {}", format_list(&.0))]
    CircularRedirect(Vec<OperationRef>),

    #[error("A circular dependency exists between operations: {}", format_list(&.0))]
    CircularOperationDependency(Vec<OperationName>),

    #[error("An error occurred while finishing the workflow build: {0}")]
    FinishingErrors(FinishingErrors),

    #[error("An error occurred while creating a scope: {0}")]
    IncrementalScopeError(#[from] IncrementalScopeError),

    #[error("Unable to infer message types within the diagram: {0}")]
    MessageTypeInferenceFailure(MessageTypeInferenceFailure),
}

impl From<DiagramErrorCode> for DiagramError {
    fn from(code: DiagramErrorCode) -> Self {
        DiagramError {
            context: DiagramErrorContext::default(),
            code,
        }
    }
}

impl DiagramErrorCode {
    pub fn operation_name_not_found(name: OperationName) -> Self {
        DiagramErrorCode::OperationNotFound(NextOperation::Name(name))
    }

    pub fn in_port(self, port_id: impl Into<PortRef>) -> DiagramError {
        DiagramError::in_port(port_id, self)
    }
}

// TODO(@mxgrey): Add explainability to this error. E.g. say in plain words why
// there was no valid choice.
#[derive(Clone, ThisError, Default)]
pub struct MessageTypeInferenceFailure {
    pub no_valid_choice: Vec<PortRef>,
    pub ambiguous_choice: Vec<(PortRef, Vec<Arc<str>>)>,
    pub constraints: HashMap<PortRef, Option<Arc<dyn MessageTypeConstraint>>>,
}

impl std::fmt::Debug for MessageTypeInferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageTypeInferenceFailure")
            .field("no_valid_choice", &self.no_valid_choice)
            .field("ambiguous_choice", &self.ambiguous_choice)
            .finish()
    }
}

impl MessageTypeInferenceFailure {
    pub fn is_failed(&self) -> bool {
        !self.no_valid_choice.is_empty() || !self.ambiguous_choice.is_empty()
    }
}

impl std::fmt::Display for MessageTypeInferenceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.no_valid_choice.is_empty() {
            write!(
                f,
                "No valid message type choices for {}. ",
                format_list(&self.no_valid_choice)
            )?;
        }

        if !self.ambiguous_choice.is_empty() {
            write!(f, "Ambiguous message type choices:")?;
            for (port, choices) in &self.ambiguous_choice {
                write!(f, "\n -- {port}: {}", format_list(choices))?;
            }
        }

        Ok(())
    }
}

impl From<TryJoinError> for DiagramErrorCode {
    fn from(value: TryJoinError) -> Self {
        match value {
            TryJoinError::IncompatibleLayout(err) => DiagramErrorCode::IncompatibleBuffers(err),
            TryJoinError::DuplicateBuffer(err) => DiagramErrorCode::DuplicateBufferInJoin(err),
        }
    }
}

/// An error that occurs when a diagram description expects a node to provide a
/// named output stream, but the node does not provide any output stream that
/// matches the expected name.
#[derive(Clone, ThisError, Debug)]
#[error("An expected stream is not provided by this node: {missing_name}. Available stream names: {}", format_list(&available_names))]
pub struct MissingStream {
    pub missing_name: OperationName,
    pub available_names: Vec<OperationName>,
}

#[derive(Clone, ThisError, Debug, Default)]
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
    fn test_disconnected_diagram() {
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
            .with_result();

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
