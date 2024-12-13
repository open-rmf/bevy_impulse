mod fork_clone;
mod fork_result;
mod impls;
mod node_registry;
mod serialization;
mod split_serialized;
mod transform;
mod unzip;
mod workflow_builder;

use fork_clone::ForkCloneOp;
use fork_result::ForkResultOp;
use log::debug;
pub use node_registry::*;
pub use serialization::*;
pub use split_serialized::*;
use transform::{TransformError, TransformOp};
use unzip::UnzipOp;
pub use workflow_builder::*;

// ----------

use std::{collections::HashMap, error::Error, fmt::Display, io::Read};

use crate::{Builder, Scope, Service, SpawnWorkflowExt, SplitConnectionError, StreamPack};
use bevy_app::App;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type NodeId = String;
pub type OperationId = String;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StartOp {
    next: OperationId,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TerminateOp {}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeOp {
    node_id: NodeId,
    #[serde(default)]
    config: serde_json::Value,
    next: OperationId,
}

#[derive(Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
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
    ///             "nodeId": "myNode",
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
    ///             "next": "forkClone"
    ///         },
    ///         "forkClone": {
    ///             "type": "forkClone",
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
    ///             "next": "forkResult"
    ///         },
    ///         "forkResult": {
    ///             "type": "forkResult",
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

type ScopeStart = serde_json::Value;
type ScopeTerminate = serde_json::Value;
type DynScope<Streams> = Scope<ScopeStart, ScopeTerminate, Streams>;

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    /// registry.register_node("echo", "echo", |builder, _config: ()| {
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
    ///             "nodeId": "echo",
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
    pub fn spawn_workflow<Streams>(
        &self,
        app: &mut App,
        registry: &NodeRegistry,
    ) -> Result<Service<ScopeStart, ScopeTerminate, Streams>, DiagramError>
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

        let w = app
            .world
            .spawn_workflow(|scope: DynScope<Streams>, builder: &mut Builder| {
                debug!(
                    "spawn workflow, scope input: {:?}, terminate: {:?}",
                    scope.input.id(),
                    scope.terminate.id()
                );

                let mut dyn_builder =
                    unwrap_or_return!(WorkflowBuilder::new(&scope, builder, registry, self));

                // connect node operations
                for (op_id, op) in self.ops.iter().filter_map(|(op_id, v)| match v {
                    DiagramOperation::Node(op) => Some((op_id, op)),
                    _ => None,
                }) {
                    debug!("connecting op [{:?}]", op_id);
                    unwrap_or_return!(dyn_builder.connect_node(&scope, builder, op_id, op));
                }

                // connect start operation, note that this consumes scope, so we need to do this last
                if let Some((op_id, start_op)) = self.ops.iter().find_map(|(op_id, v)| match v {
                    DiagramOperation::Start(op) => Some((op_id, op)),
                    _ => None,
                }) {
                    debug!("connecting op [{:?}]", op_id);
                    unwrap_or_return!(dyn_builder.connect_start(scope, builder, start_op));
                }
            });

        if let Some(err) = err {
            return Err(err);
        }

        Ok(w)
    }

    /// Wrapper to [`spawn_workflow::<()>`].
    pub fn spawn_io_workflow(
        &self,
        app: &mut App,
        registry: &NodeRegistry,
    ) -> Result<Service<ScopeStart, ScopeTerminate, ()>, DiagramError> {
        self.spawn_workflow::<()>(app, registry)
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

    fn get_op(&self, op_id: &OperationId) -> Result<&DiagramOperation, DiagramError> {
        self.ops
            .get(op_id)
            .ok_or_else(|| DiagramError::OperationNotFound(op_id.clone()))
    }
}

#[derive(Debug)]
pub enum DiagramError {
    NodeNotFound(NodeId),
    OperationNotFound(OperationId),
    TypeMismatch,
    MissingStartOrTerminate,
    CannotConnectStart,
    NotSerializable,
    NotCloneable,
    NotUnzippable,
    CannotForkResult,
    NotSplittable,
    CannotTransform(TransformError),
    BadInterconnectChain,
    JsonError(serde_json::Error),
    ConnectionError(Box<dyn Error>),
}

impl Display for DiagramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound(node_id) => write!(f, "node [{}] is not registered", node_id),
            Self::OperationNotFound(op_id) => write!(f, "operation [{}] not found", op_id),
            Self::TypeMismatch => f.write_str("output type does not match input type"),
            Self::MissingStartOrTerminate => f.write_str("missing start or terminate"),
            Self::CannotConnectStart => f.write_str("cannot connect to start"),
            Self::NotSerializable => {
                f.write_str("request or response cannot be serialized or deserialized")
            }
            Self::NotCloneable => f.write_str("response cannot be cloned"),
            Self::NotUnzippable => f.write_str(
                "the number of unzip slots in response does not match the number of inputs",
            ),
            Self::CannotForkResult => f.write_str(
                "node must be registered with \"with_fork_result()\" to be able to perform fork result",
            ),
            Self::NotSplittable => f.write_str("response cannot be splitted"),
            Self::CannotTransform(err) => err.fmt(f),
            Self::BadInterconnectChain => {
                f.write_str("an interconnect like forkClone cannot connect to another interconnect")
            }
            Self::JsonError(err) => err.fmt(f),
            Self::ConnectionError(inner) => inner.fmt(f),
        }
    }
}

impl Error for DiagramError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::JsonError(err) => Some(err),
            Self::ConnectionError(inner) => Some(inner.as_ref()),
            Self::CannotTransform(inner) => Some(inner),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for DiagramError {
    fn from(err: serde_json::Error) -> Self {
        DiagramError::JsonError(err)
    }
}

impl From<SplitConnectionError> for DiagramError {
    fn from(value: SplitConnectionError) -> Self {
        DiagramError::ConnectionError(value.into())
    }
}

impl From<TransformError> for DiagramError {
    fn from(value: TransformError) -> Self {
        DiagramError::CannotTransform(value)
    }
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
                    "nodeId": "multiply3",
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
        assert!(
            matches!(
                *err.downcast_ref::<Cancellation>().unwrap().cause,
                CancellationCause::Unreachable(_)
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
                    "nodeId": "multiply3",
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
        assert!(
            matches!(
                *err.downcast_ref::<Cancellation>().unwrap().cause,
                CancellationCause::Unreachable(_)
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
                    "nodeId": "multiply3_cloneable",
                    "next": "forkClone",
                },
                "forkClone": {
                    "type": "forkClone",
                    "next": ["start", "terminate"],
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
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
                    "nodeId": "opaque_request",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
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
                    "nodeId": "opaque_response",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
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
                    "nodeId": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "nodeId": "opaque_request",
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
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
                    "nodeId": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "nodeId": "multiply3",
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
                    "nodeId": "multiply3_cloneable",
                    "next": "forkClone",
                },
                "forkClone": {
                    "type": "forkClone",
                    "next": ["op1", "op2"],
                },
                "op2": {
                    "type": "node",
                    "nodeId": "multiply3",
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
                    "nodeId": "multiplyBy",
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
                    "nodeId": "multiply3_5",
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
