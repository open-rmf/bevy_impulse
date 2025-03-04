mod buffer;
mod fork_clone;
mod fork_result;
mod impls;
mod join;
mod node;
mod registration;
mod section;
mod serialization;
mod split_serialized;
mod transform;
mod type_info;
mod unzip;
mod workflow_builder;

use bevy_ecs::system::Commands;
use buffer::{BufferAccessOp, BufferOp, ListenOp};
use fork_clone::ForkCloneOp;
use fork_result::ForkResultOp;
pub use join::JoinOutput;
use join::{JoinOp, SerializedJoinOp};
pub use node::NodeOp;
pub use registration::*;
pub use section::*;
pub use serialization::*;
pub use split_serialized::*;
use tracing::debug;
use transform::{TransformError, TransformOp};
use type_info::TypeInfo;
use unzip::UnzipOp;
pub use workflow_builder::*;

// ----------

use std::{collections::HashMap, fmt::Display, io::Read};

use crate::{
    Builder, IncompatibleLayout, Scope, Service, SpawnWorkflowExt, SplitConnectionError, StreamPack,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const SUPPORTED_DIAGRAM_VERSION: &str = ">=0.1.0, <0.2.0";

pub type BuilderId = String;
pub type OperationId = String;

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum NextOperation {
    Target(OperationId),
    Section { section: OperationId, input: String },
    Builtin { builtin: BuiltinTarget },
}

impl Display for NextOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(operation_id) => f.write_str(operation_id),
            Self::Section { section, input } => write!(f, "section:{}({})", section, input),
            Self::Builtin { builtin } => write!(f, "builtin:{}", builtin),
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
    /// Use the output to terminate the workflow. This will be the return value
    /// of the workflow.
    Terminate,

    /// Dispose of the output.
    Dispose,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum SourceOperation {
    Source(OperationId),
    Builtin { builtin: BuiltinSource },
}

impl From<OperationId> for SourceOperation {
    fn from(value: OperationId) -> Self {
        SourceOperation::Source(value)
    }
}

impl Display for SourceOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(operation_id) => f.write_str(operation_id),
            Self::Builtin { builtin } => write!(f, "builtin:{}", builtin),
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
pub enum BuiltinSource {
    Start,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TerminateOp {}

#[derive(Clone, strum::Display, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[strum(serialize_all = "snake_case")]
pub enum DiagramOperation {
    /// Connect the request to a registered node.
    ///
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "node_op",
    ///     "ops": {
    ///         "node_op": {
    ///             "type": "node",
    ///             "builder": "my_node_builder",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Node(NodeOp),

    /// Connect the request to a registered section.
    ///
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "section_op",
    ///     "ops": {
    ///         "section_op": {
    ///             "type": "section",
    ///             "builder": "my_section_builder",
    ///             "connect": {
    ///                 "my_section_output": { "builtin": "terminate" }
    ///             }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Section(SectionOp),

    /// If the request is cloneable, clone it into multiple responses.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["terminate"]
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    ForkClone(ForkCloneOp),

    /// If the request is a tuple of (T1, T2, T3, ...), unzip it into multiple responses
    /// of T1, T2, T3, ...
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "unzip",
    ///     "ops": {
    ///         "unzip": {
    ///             "type": "unzip",
    ///             "next": [{ "builtin": "terminate" }]
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Unzip(UnzipOp),

    /// If the request is a `Result<_, _>`, branch it to `Ok` and `Err`.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_result",
    ///     "ops": {
    ///         "fork_result": {
    ///             "type": "fork_result",
    ///             "ok": { "builtin": "terminate" },
    ///             "err": { "builtin": "dispose" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    ForkResult(ForkResultOp),

    /// If the request is a list-like or map-like object, split it into multiple responses.
    /// Note that the split output is a tuple of `(KeyOrIndex, Value)`, nodes receiving a split
    /// output should have request of that type instead of just the value type.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "split",
    ///     "ops": {
    ///         "split": {
    ///             "type": "split",
    ///             "index": [{ "builtin": "terminate" }]
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Split(SplitOp),

    /// Wait for an item to be emitted from each of the inputs, then combine the
    /// oldest of each into an array.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["foo", "bar"]
    ///         },
    ///         "foo": {
    ///             "type": "node",
    ///             "builder": "foo",
    ///             "next": "foo_buffer"
    ///         },
    ///         "foo_buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "bar": {
    ///             "type": "node",
    ///             "builder": "bar",
    ///             "next": "bar_buffer"
    ///         },
    ///         "bar_buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "join": {
    ///             "type": "join",
    ///             "buffers": {
    ///                 "foo": "foo_buffer",
    ///                 "bar": "bar_buffer"
    ///             },
    ///             "target_node": "foobar",
    ///             "next": "foobar"
    ///         },
    ///         "foobar": {
    ///             "type": "node",
    ///             "builder": "foobar",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Join(JoinOp),

    /// Wait for an item to be emitted from each of the inputs, then combine the
    /// oldest of each into an array. Unlike `join`, this only works with serialized buffers.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["foo", "bar"]
    ///         },
    ///         "foo": {
    ///             "type": "node",
    ///             "builder": "foo",
    ///             "next": "foo_buffer"
    ///         },
    ///         "foo_buffer": {
    ///             "type": "buffer",
    ///             "serialize": true
    ///         },
    ///         "bar": {
    ///             "type": "node",
    ///             "builder": "bar",
    ///             "next": "bar_buffer"
    ///         },
    ///         "bar_buffer": {
    ///             "type": "buffer",
    ///             "serialize": true
    ///         },
    ///         "serialized_join": {
    ///             "type": "serialized_join",
    ///             "buffers": {
    ///                 "foo": "foo_buffer",
    ///                 "bar": "bar_buffer"
    ///             },
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    SerializedJoin(SerializedJoinOp),

    /// If the request is serializable, transform it by running it through a [CEL](https://cel.dev/) program.
    /// The context includes a "request" variable which contains the request.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "transform",
    ///     "ops": {
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "request.name",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    ///
    /// Note that due to how `serde_json` performs serialization, positive integers are always
    /// serialized as unsigned. In CEL, You can't do an operation between unsigned and signed so
    /// it is recommended to always perform explicit casts.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "transform",
    ///     "ops": {
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "int(request.score) * 3",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Transform(TransformOp),

    /// Create a [`crate::Buffer`] which can be used to store and pull data within
    /// a scope.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["num_output", "string_output"]
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer_access"
    ///         },
    ///         "string_output": {
    ///             "type": "node",
    ///             "builder": "string_output",
    ///             "next": "string_buffer"
    ///         },
    ///         "string_buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "buffer_access": {
    ///             "type": "buffer_access",
    ///             "buffers": ["string_buffer"],
    ///             "target_node": "with_buffer_access",
    ///             "next": "with_buffer_access"
    ///         },
    ///         "with_buffer_access": {
    ///             "type": "node",
    ///             "builder": "with_buffer_access",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Buffer(BufferOp),

    /// Zip a response with a buffer access.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["num_output", "string_output"]
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer_access"
    ///         },
    ///         "string_output": {
    ///             "type": "node",
    ///             "builder": "string_output",
    ///             "next": "string_buffer"
    ///         },
    ///         "string_buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "buffer_access": {
    ///             "type": "buffer_access",
    ///             "buffers": ["string_buffer"],
    ///             "target_node": "with_buffer_access",
    ///             "next": "with_buffer_access"
    ///         },
    ///         "with_buffer_access": {
    ///             "type": "node",
    ///             "builder": "with_buffer_access",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    BufferAccess(BufferAccessOp),

    /// Listen on a buffer.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "num_output",
    ///     "ops": {
    ///         "buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer"
    ///         },
    ///         "listen": {
    ///             "type": "listen",
    ///             "buffers": ["buffer"],
    ///             "target_node": "listen_buffer",
    ///             "next": "listen_buffer"
    ///         },
    ///         "listen_buffer": {
    ///             "type": "node",
    ///             "builder": "listen_buffer",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Listen(ListenOp),
}

type DiagramStart = serde_json::Value;
type DiagramTerminate = serde_json::Value;
type DiagramScope<Streams = ()> = Scope<DiagramStart, DiagramTerminate, Streams>;

/// Returns the schema for [`String`]
fn schema_with_string(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    gen.subschema_for::<String>()
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

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagram {
    /// Version of the diagram, should always be `0.1.0`.
    #[serde(
        deserialize_with = "deserialize_semver",
        serialize_with = "serialize_semver"
    )]
    #[schemars(schema_with = "schema_with_string")]
    version: semver::Version,

    /// Signifies the start of a workflow.
    start: NextOperation,

    ops: HashMap<OperationId, DiagramOperation>,
}

impl Diagram {
    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::{Diagram, DiagramError, NodeBuilderOptions, DiagramElementRegistry, RunCommandsOnWorldExt};
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
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    // TODO(koonpeng): Support streams other than `()` #43.
    /* pub */
    fn spawn_workflow<Streams>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<DiagramStart, DiagramTerminate, Streams>, DiagramError>
    where
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

        let w = cmds.spawn_workflow(|scope: DiagramScope<Streams>, builder: &mut Builder| {
            debug!(
                "spawn workflow, scope input: {:?}, terminate: {:?}",
                scope.input.id(),
                scope.terminate.id()
            );

            unwrap_or_return!(create_workflow(scope, builder, registry, self));
        });

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
    /// use bevy_impulse::{Diagram, DiagramError, NodeBuilderOptions, DiagramElementRegistry, RunCommandsOnWorldExt};
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
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn spawn_io_workflow(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<DiagramStart, DiagramTerminate, ()>, DiagramError> {
        self.spawn_workflow::<()>(cmds, registry)
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

    fn get_op(&self, op_id: &OperationId) -> Result<&DiagramOperation, DiagramErrorCode> {
        self.ops
            .get(op_id)
            .ok_or_else(|| DiagramErrorCode::OperationNotFound(op_id.clone()))
    }
}

#[derive(Debug)]
pub struct DiagramErrorContext {
    op_id: Option<OperationId>,
}

impl Display for DiagramErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(op_id) = &self.op_id {
            write!(f, "in operation [{}],", op_id)?;
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{context} {code}")]
pub struct DiagramError {
    pub context: DiagramErrorContext,

    #[source]
    pub code: DiagramErrorCode,
}

#[derive(thiserror::Error, Debug)]
pub enum DiagramErrorCode {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("operation [{0}] not found")]
    OperationNotFound(OperationId),

    #[error("type mismatch, source {source_type}, target {target_type}")]
    TypeMismatch {
        source_type: TypeInfo,
        target_type: TypeInfo,
    },

    #[error("missing start or terminate")]
    MissingStartOrTerminate,

    #[error("cannot connect to start")]
    CannotConnectStart,

    #[error("request or response cannot be serialized or deserialized")]
    NotSerializable,

    #[error("response cannot be cloned")]
    NotCloneable,

    #[error("the number of unzip slots in response does not match the number of inputs")]
    NotUnzippable,

    #[error(
        "node must be registered with \"with_fork_result()\" to be able to perform fork result"
    )]
    CannotForkResult,

    #[error("response cannot be split")]
    NotSplittable,

    #[error("message cannot be joined from the input buffers")]
    NotJoinable,

    #[error("empty join is not allowed")]
    EmptyJoin,

    #[error("join target type cannot be determined from [next] and [target_node] is not provided")]
    UnknownTarget,

    #[error(transparent)]
    CannotTransform(#[from] TransformError),

    #[error("box/unbox operation for the message is not registered")]
    CannotBoxOrUnbox,

    #[error("cannot access buffer")]
    CannotBufferAccess,

    #[error("cannot listen")]
    CannotListen,

    #[error(transparent)]
    IncompatibleBuffers(#[from] IncompatibleLayout),

    #[error(transparent)]
    SectionError(#[from] SectionError),

    #[error("one or more operation is missing inputs")]
    IncompleteDiagram,

    #[error("operation type only accept single input")]
    OnlySingleInput,

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),

    /// Use this only for errors that *should* never happen because of some preconditions.
    /// If this error ever comes up, then it likely means that there is some logical flaws
    /// in the algorithm.
    #[error("an unknown error occurred while building the diagram, {0}")]
    UnknownError(String),
}

#[macro_export]
macro_rules! unknown_diagram_error {
    () => {
        DiagramErrorCode::UnknownError(format!("{}:{}", file!(), line!()))
    };
}

#[cfg(test)]
mod testing;

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
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
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

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable),
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

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable),
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

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(
                err.code,
                DiagramErrorCode::TypeMismatch {
                    target_type: _,
                    source_type: _
                }
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
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
        assert!(matches!(
            *err.downcast_ref::<Cancellation>().unwrap().cause,
            CancellationCause::Unreachable(_)
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
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

        let result = fixture
            .spawn_and_run(
                &Diagram::from_json_str(json_str).unwrap(),
                serde_json::Value::from(4),
            )
            .unwrap();
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
                    "next": ["transform"],
                },
                "transform": {
                    "type": "transform",
                    "cel": "777",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 777);
    }
}
