mod fork_clone;
mod fork_result;
mod impls;
mod node_registry;
mod serialization;
mod split_serialized;
mod transform;
mod unzip;
mod workflow_builder;

use log::debug;
pub use node_registry::*;
pub use serialization::*;
pub use split_serialized::*;
use transform::{TransformError, TransformOp};
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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkCloneOp {
    next: Vec<OperationId>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnzipOp {
    next: Vec<OperationId>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkResultOp {
    ok: OperationId,
    err: OperationId,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum SplitOpParams {
    Index(Vec<OperationId>),
    Key(HashMap<String, OperationId>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SplitOp {
    #[serde(flatten)]
    params: SplitOpParams,

    remaining: Option<OperationId>,
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "nodeOp"
    ///     },
    ///     "nodeOp": {
    ///         "type": "node",
    ///         "nodeId": "myNode",
    ///         "next": "terminate"
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "forkClone"
    ///     },
    ///     "forkClone": {
    ///         "type": "forkClone",
    ///         "next": ["terminate"]
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "unzip"
    ///     },
    ///     "unzip": {
    ///         "type": "unzip",
    ///         "next": ["terminate"]
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "forkResult"
    ///     },
    ///     "forkResult": {
    ///         "type": "forkResult",
    ///         "ok": "terminate",
    ///         "err": "dispose"
    ///     },
    ///     "dispose": {
    ///         "type": "dispose"
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "split"
    ///     },
    ///     "split": {
    ///         "type": "split",
    ///         "index": ["terminate"]
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "transform"
    ///     },
    ///     "transform": {
    ///         "type": "transform",
    ///         "cel": "request.name",
    ///         "next": "terminate"
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "transform"
    ///     },
    ///     "transform": {
    ///         "type": "transform",
    ///         "cel": "int(request.score) * 3",
    ///         "next": "terminate"
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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
    #[serde(flatten)]
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
    ///     "start": {
    ///         "type": "start",
    ///         "next": "echo"
    ///     },
    ///     "echo": {
    ///         "type": "node",
    ///         "nodeId": "echo",
    ///         "next": "terminate"
    ///     },
    ///     "terminate": {
    ///         "type": "terminate"
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

    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        Ok(serde_json::from_str(s)?)
    }

    pub fn from_reader<R>(r: R) -> Result<Self, serde_json::Error>
    where
        R: Read,
    {
        Ok(serde_json::from_reader(r)?)
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
    use crate::{testing::TestingContext, CancellationCause, RequestExt};
    use test_log::test;
    use testing::{new_registry_with_basic_nodes, unwrap_promise};

    use super::*;

    #[test]
    fn test_no_terminate() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_1".to_string(),
                    }),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_no_start() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_connect_to_start() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["start".to_string(), "terminate".to_string()],
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::CannotConnectStart), "{:?}", err);
    }

    #[test]
    fn test_unserializable_start() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "opaque_request".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
    }

    #[test]
    fn test_unserializable_terminate() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "opaque_response".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
    }

    #[test]
    fn test_mismatch_types() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "opaque_request".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::TypeMismatch), "{:?}", err);
    }

    #[test]
    fn test_disconnected() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_fork_clone_uncloneable() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_3".to_string(), "terminate".to_string()],
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotCloneable), "{:?}", err);
    }

    #[test]
    fn test_fork_clone() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_3".to_string(), "terminate".to_string()],
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 12);
    }

    #[test]
    fn test_unzip_not_unzippable() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["op_2".to_string()],
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable));
    }

    #[test]
    fn test_unzip_to_too_many_slots() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["op_2".to_string(), "op_3".to_string(), "op_4".to_string()],
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_4".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable));
    }

    #[test]
    fn test_unzip_to_terminate() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["dispose".to_string(), "terminate".to_string()],
                    }),
                ),
                ("dispose".to_string(), DiagramOperation::Dispose),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 20);
    }

    #[test]
    fn test_unzip() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["op_2".to_string()],
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 36);
    }

    #[test]
    fn test_unzip_with_dispose() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["dispose".to_string(), "op_2".to_string()],
                    }),
                ),
                ("dispose".to_string(), DiagramOperation::Dispose),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 60);
    }

    #[test]
    fn test_fork_result() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn check_even(v: i64) -> Result<String, String> {
            if v % 2 == 0 {
                Ok("even".to_string())
            } else {
                Err("odd".to_string())
            }
        }

        registry
            .registration_builder()
            .with_fork_result()
            .register_node(
                "check_even",
                "check_even",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&check_even),
            );

        fn echo(s: String) -> String {
            s
        }

        registry.register_node("echo", "echo", |builder: &mut Builder, _config: ()| {
            builder.create_map_block(&echo)
        });

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "check_even".to_string(),
                        config: serde_json::Value::Null,
                        next: "fork_result".to_string(),
                    }),
                ),
                (
                    "fork_result".to_string(),
                    DiagramOperation::ForkResult(ForkResultOp {
                        ok: "op_2".to_string(),
                        err: "op_3".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "echo".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "echo".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), "even");

        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(3), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), "odd");
    }

    #[test]
    fn test_split_list() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        registry
            .registration_builder()
            .with_splittable()
            .register_node(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "split_list".to_string(),
                        config: serde_json::Value::Null,
                        next: "split".to_string(),
                    }),
                ),
                (
                    "split".to_string(),
                    DiagramOperation::Split(SplitOp {
                        params: SplitOpParams::Index(vec!["terminate".to_string()]),
                        remaining: None,
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap()[1], 1);
    }

    #[test]
    fn test_split_list_with_key() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        registry
            .registration_builder()
            .with_splittable()
            .register_node(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "split_list".to_string(),
                        config: serde_json::Value::Null,
                        next: "split".to_string(),
                    }),
                ),
                (
                    "split".to_string(),
                    DiagramOperation::Split(SplitOp {
                        params: SplitOpParams::Key(HashMap::from([(
                            "1".to_string(),
                            "terminate".to_string(),
                        )])),
                        remaining: None,
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap()[1], 2);
    }

    #[test]
    fn test_split_map() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn split_map(_: i64) -> HashMap<String, i64> {
            HashMap::from([
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3),
            ])
        }

        registry
            .registration_builder()
            .with_splittable()
            .register_node(
                "split_map",
                "split_map",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_map),
            );

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "split_map".to_string(),
                        config: serde_json::Value::Null,
                        next: "split".to_string(),
                    }),
                ),
                (
                    "split".to_string(),
                    DiagramOperation::Split(SplitOp {
                        params: SplitOpParams::Key(HashMap::from([(
                            "b".to_string(),
                            "terminate".to_string(),
                        )])),
                        remaining: None,
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap()[1], 2);
    }

    #[test]
    fn test_split_remaining() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        registry
            .registration_builder()
            .with_splittable()
            .register_node(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "split_list".to_string(),
                        config: serde_json::Value::Null,
                        next: "split".to_string(),
                    }),
                ),
                (
                    "split".to_string(),
                    DiagramOperation::Split(SplitOp {
                        params: SplitOpParams::Index(vec!["dispose".to_string()]),
                        remaining: Some("terminate".to_string()),
                    }),
                ),
                ("dispose".to_string(), DiagramOperation::Dispose),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap()[1], 2);
    }

    #[test]
    fn test_looping_diagram() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        config: serde_json::Value::Null,
                        next: "fork_clone".to_string(),
                    }),
                ),
                (
                    "fork_clone".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_1".to_string(), "op_2".to_string()],
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 36);
    }

    #[test]
    fn test_noop_diagram() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 4);
    }

    #[test]
    fn test_serialized_diagram() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let json_str = r#"
        {
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
        "#;

        let diagram = Diagram::from_json_str(json_str).unwrap();
        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 28);
    }

    /// Test that we can transform on a slot of a unzipped response. Operations which changes
    /// the output type has extra serialization logic.
    #[test]
    fn test_transform_unzip() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["transform".to_string()],
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        cel: "777".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 777);
    }
}
