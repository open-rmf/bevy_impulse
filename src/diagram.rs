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
pub use node_registry::*;
pub use serialization::*;
pub use split_serialized::*;
use tracing::debug;
use transform::{TransformError, TransformOp};
use unzip::UnzipOp;
use workflow_builder::create_workflow;

// ----------

use std::{collections::HashMap, io::Read};

use crate::{Builder, Scope, Service, SpawnWorkflowExt, SplitConnectionError, StreamPack};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type BuilderId = String;
pub type OperationId = String;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct StartOp {
    next: OperationId,
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
    next: OperationId,
}

#[derive(Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DiagramOperation {
    /// Signifies the start of a workflow. There must be exactly 1 start operation in a diagram.
    Start(StartOp),

    /// Signifies the end of a workflow. There must be exactly 1 terminate operation in a diagram.
    Terminate(TerminateOp),

    /// Connects the request to a registered node.
    ///
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "nodeOp"
    ///         },
    ///         "nodeOp": {
    ///             "type": "node",
    ///             "builder": "myNode",
    ///             "next": "terminate"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
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
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "fork_clone"
    ///         },
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["terminate"]
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
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
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "unzip"
    ///         },
    ///         "unzip": {
    ///             "type": "unzip",
    ///             "next": ["terminate"]
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
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
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "fork_result"
    ///         },
    ///         "fork_result": {
    ///             "type": "fork_result",
    ///             "ok": "terminate",
    ///             "err": "dispose"
    ///         },
    ///         "dispose": {
    ///             "type": "dispose"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    ForkResult(ForkResultOp),

    /// If the request is a list-like or map-like object, splits it into multiple responses.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "split"
    ///         },
    ///         "split": {
    ///             "type": "split",
    ///             "index": ["terminate"]
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Split(SplitOp),

    /// Waits for an item to be emitted from each of the inputs, then combined the
    /// oldest of each into an array.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "split"
    ///         },
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
    ///             "next": "terminate"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Join(JoinOp),

    /// If the request is serializable, transforms it by running it through a [CEL](https://cel.dev/) program.
    /// The context includes a "request" variable which contains the request.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "transform"
    ///         },
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "request.name",
    ///             "next": "terminate"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
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
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "transform"
    ///         },
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "int(request.score) * 3",
    ///             "next": "terminate"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Transform(TransformOp),

    /// Drops the request, equivalent to a no-op.
    Dispose,
}

type DiagramStart = serde_json::Value;
type DiagramTerminate = serde_json::Value;
type DiagramScope<Streams = ()> = Scope<DiagramStart, DiagramTerminate, Streams>;

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagram {
    ops: HashMap<OperationId, DiagramOperation>,
}

impl Diagram {
    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::{Diagram, DiagramError, NodeRegistry};
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = NodeRegistry::default();
    /// registry.register_node_builder("echo", "echo", |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| msg)
    /// });
    ///
    /// let json_str = r#"
    /// {
    ///     "ops": {
    ///         "start": {
    ///             "type": "start",
    ///             "next": "echo"
    ///         },
    ///         "echo": {
    ///             "type": "node",
    ///             "builder": "echo",
    ///             "next": "terminate"
    ///         },
    ///         "terminate": {
    ///             "type": "terminate"
    ///         }
    ///     }
    /// }
    /// "#;
    ///
    /// let diagram = Diagram::from_json_str(json_str)?;
    /// let workflow = diagram.spawn_io_workflow(&mut app, &registry)?;
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

    #[error("response cannot be splitted")]
    NotSplittable,

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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "dispose",
                },
                "dispose": {
                    "type": "dispose",
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
        assert!(err.downcast_ref::<DiagramError>().is_some(), "{:?}", err);
        assert!(
            matches!(
                err.downcast_ref::<DiagramError>().unwrap(),
                DiagramError::MissingStartOrTerminate,
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_no_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
        assert!(err.downcast_ref::<DiagramError>().is_some(), "{:?}", err);
        assert!(
            matches!(
                err.downcast_ref::<DiagramError>().unwrap(),
                DiagramError::MissingStartOrTerminate,
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_connect_to_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_cloneable",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["start", "terminate"],
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err, DiagramError::CannotConnectStart), "{:?}", err);
    }

    #[test]
    fn test_unserializable_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "opaque_response",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate",
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
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
                "terminate": {
                    "type": "terminate",
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_cloneable",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op1", "op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "multiply3"
                },
                "multiply3": {
                    "type": "node",
                    "builder": "multiplyBy",
                    "config": 7,
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate"
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
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
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
