mod fork_clone;
mod fork_result;
mod impls;
mod join;
mod node_registry;
mod serialization;
mod split_serialized;
mod transform;
mod unzip;
mod workflow_builder;

use bevy_ecs::system::Commands;
use fork_clone::ForkCloneOp;
use fork_result::ForkResultOp;
use join::JoinOp;
pub use join::JoinOutput;
pub use node_registry::*;
pub use serialization::*;
pub use split_serialized::*;
use tracing::debug;
use transform::{TransformError, TransformOp};
use unzip::UnzipOp;
use workflow_builder::create_workflow;

// ----------

use std::{collections::HashMap, fmt::Display, io::Read};

use crate::{Builder, Scope, Service, SpawnWorkflowExt, SplitConnectionError, StreamPack};
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
    Builtin { builtin: BuiltinTarget },
}

impl Display for NextOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(operation_id) => f.write_str(operation_id),
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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeOp {
    builder: BuilderId,
    #[serde(default)]
    config: serde_json::Value,
    next: NextOperation,
}

#[derive(Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
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

    /// Wait for an item to be emitted from each of the inputs, then combined the
    /// oldest of each into an array.
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
    ///             "index": ["op1", "op2"]
    ///         },
    ///         "op1": {
    ///             "type": "node",
    ///             "builder": "foo",
    ///             "next": "join"
    ///         },
    ///         "op2": {
    ///             "type": "node",
    ///             "builder": "bar",
    ///             "next": "join"
    ///         },
    ///         "join": {
    ///             "type": "join",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Join(JoinOp),

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

    /// Drop the request, equivalent to a no-op.
    Dispose,
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
    /// use bevy_impulse::{Diagram, DiagramError, NodeRegistry, RunCommandsOnWorldExt};
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = NodeRegistry::default();
    /// registry.register_node_builder("echo".to_string(), "echo".to_string(), |builder, _config: ()| {
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
    /// # Ok::<_, DiagramError>(())
    /// ```
    // TODO(koonpeng): Support streams other than `()` #43.
    /* pub */
    fn spawn_workflow<Streams>(
        &self,
        cmds: &mut Commands,
        registry: &NodeRegistry,
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

    /// Wrapper to [spawn_workflow::<()>](Self::spawn_workflow).
    pub fn spawn_io_workflow(
        &self,
        cmds: &mut Commands,
        registry: &NodeRegistry,
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
}

#[derive(thiserror::Error, Debug)]
pub enum DiagramError {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("operation [{0}] not found")]
    OperationNotFound(OperationId),

    #[error("output type does not match input type")]
    TypeMismatch,

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

    #[error("empty join is not allowed")]
    EmptyJoin,

    #[error(transparent)]
    CannotTransform(#[from] TransformError),

    #[error("an interconnect like fork_clone cannot connect to another interconnect")]
    BadInterconnectChain,

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),
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
                    "builder": "multiply3_uncloneable",
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
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
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
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
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
                    "builder": "multiply3_uncloneable",
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
        assert!(matches!(err, DiagramError::TypeMismatch), "{:?}", err);
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
                    "builder": "multiply3_uncloneable",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
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
                    "builder": "multiply3_uncloneable",
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
            "start": "multiply3_uncloneable",
            "ops": {
                "multiply3_uncloneable": {
                    "type": "node",
                    "builder": "multiplyBy",
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
