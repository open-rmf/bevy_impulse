mod node_registry;
mod serialization;
mod workflow_builder;

pub use node_registry::*;
pub use serialization::*;
pub use workflow_builder::*;

// ----------

use std::{collections::HashMap, error::Error, fmt::Display, io::Read};

use crate::{Builder, Scope, Service, SpawnWorkflowExt, StreamPack};
use bevy_app::App;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub type NodeId = String;
pub type OperationId = String;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StartOp {
    next: OperationId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TerminateOp {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeOp {
    node_id: NodeId,
    next: OperationId,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkCloneOp {
    next: Vec<OperationId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnzipOp {
    next: Vec<OperationId>,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DiagramOperation {
    Start(StartOp),
    Terminate(TerminateOp),
    Node(NodeOp),
    ForkClone(ForkCloneOp),
    Unzip(UnzipOp),
}

type ScopeStart = serde_json::Value;
type ScopeTerminate = Result<serde_json::Value, Box<dyn Error + Send + Sync>>;
type DynScope<Streams> = Scope<ScopeStart, ScopeTerminate, Streams>;

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagram {
    #[serde(flatten)]
    ops: HashMap<OperationId, DiagramOperation>,
}

impl Diagram {
    pub fn spawn_workflow<Streams>(
        &self,
        app: &mut App,
        registry: &mut NodeRegistry,
    ) -> Service<ScopeStart, ScopeTerminate, Streams>
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
                let mut builder =
                    unwrap_or_return!(WorkflowBuilder::new(&scope, builder, registry, self));

                // connect node operations
                for op in self.ops.iter().filter_map(|(_, v)| match v {
                    DiagramOperation::Node(op) => Some(op),
                    _ => None,
                }) {
                    unwrap_or_return!(builder.connect_node(&scope, op));
                }

                // connect start operation, note that this consumes scope, so we need to do this last
                if let Some(start_op) = self.ops.iter().find_map(|(_, v)| match v {
                    DiagramOperation::Start(op) => Some(op),
                    _ => None,
                }) {
                    unwrap_or_return!(builder.connect_start(scope, start_op));
                }
            });

        if let Some(err) = err {
            return app.world.spawn_workflow(
                move |scope: DynScope<Streams>, builder: &mut Builder| {
                    let n = builder.create_map_block(move |_: ScopeStart| -> ScopeTerminate {
                        Err(Box::new(err.clone()))
                    });
                    builder.connect(scope.input, n.input);
                    builder.connect(n.output, scope.terminate);
                },
            );
        }

        w
    }

    pub fn spawn_io_workflow(
        &self,
        app: &mut App,
        registry: &mut NodeRegistry,
    ) -> Service<ScopeStart, ScopeTerminate, ()> {
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

#[derive(Clone, Debug)]
pub struct TypeMismatch {
    output_type: String,
    input_type: String,
}

#[derive(Clone, Debug)]
pub enum DiagramError {
    NodeNotFound(NodeId),
    OperationNotFound(OperationId),
    TypeMismatch(TypeMismatch),
    MissingStartOrTerminate,
    CannotConnectStart,
    NotSerializable,
    NotCloneable,
    NotUnzippable,
    BadInterconnectChain,
}

impl Display for DiagramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound(node_id) => write!(f, "node [{}] is not registered", node_id),
            Self::OperationNotFound(op_id) => write!(f, "operation [{}] not found", op_id),
            Self::TypeMismatch(data) => write!(
                f,
                "output type [{}] does not match input type [{}]",
                data.output_type, data.input_type
            ),
            Self::MissingStartOrTerminate => f.write_str("missing start or terminate"),
            Self::CannotConnectStart => f.write_str("cannot connect to start"),
            Self::NotSerializable => {
                f.write_str("request or response cannot be serialized or deserialized")
            }
            Self::NotCloneable => f.write_str("response cannot be cloned"),
            Self::NotUnzippable => f.write_str(
                "the number of unzip slots in response does not match the number of inputs",
            ),
            Self::BadInterconnectChain => {
                f.write_str("an interconnect like forkClone cannot connect to another interconnect")
            }
        }
    }
}

impl Error for DiagramError {}

#[cfg(test)]
mod tests {
    use crate::{testing::TestingContext, CancellationCause, RequestExt};

    use super::*;

    fn multiply3(i: i64) -> i64 {
        i * 3
    }

    struct Unserializable;

    fn opaque(_: Unserializable) -> Unserializable {
        Unserializable {}
    }

    fn opaque_request(_: Unserializable) {}

    fn opaque_response(_: i64) -> Unserializable {
        Unserializable {}
    }

    /// create a new node registry with some basic nodes registered
    fn new_registry_with_basic_nodes() -> NodeRegistry {
        let mut registry = NodeRegistry::default();
        registry.register_node(
            "multiply3",
            "multiply3",
            (|builder: &mut Builder| builder.create_map_block(multiply3))
                .into_registration_builder(),
        );
        registry.register_node(
            "multiply3_cloneable",
            "multiply3_cloneable",
            (|builder: &mut Builder| builder.create_map_block(multiply3))
                .into_registration_builder()
                .with_response_cloneable(),
        );
        registry.register_node(
            "opaque",
            "opaque",
            (|builder: &mut Builder| builder.create_map_block(opaque))
                .into_registration_builder()
                .with_opaque_request()
                .with_opaque_response(),
        );
        registry.register_node(
            "opaque_request",
            "opaque_request",
            (|builder: &mut Builder| builder.create_map_block(opaque_request))
                .into_registration_builder()
                .with_opaque_request(),
        );
        registry.register_node(
            "opaque_response",
            "opaque_response",
            (|builder: &mut Builder| builder.create_map_block(opaque_response))
                .into_registration_builder()
                .with_opaque_response(),
        );
        registry
    }

    #[test]
    fn test_diagram_no_terminate() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "op_1".to_string(),
                    }),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_diagram_no_start() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
            ops: HashMap::from([
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_diagram_connect_to_start() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        let err = result.unwrap_err();
        let err = err.downcast_ref::<DiagramError>().unwrap();
        assert!(matches!(err, DiagramError::CannotConnectStart), "{:?}", err);
    }

    #[test]
    fn test_diagram_unserializable_start() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        let err = result.unwrap_err();
        let err = err.downcast_ref::<DiagramError>().unwrap();
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
    }

    #[test]
    fn test_diagram_unserializable_terminate() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        let err = result.unwrap_err();
        let err = err.downcast_ref::<DiagramError>().unwrap();
        assert!(matches!(err, DiagramError::NotSerializable), "{:?}", err);
    }

    #[test]
    fn test_diagram_mismatch_types() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "opaque_request".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        let err = result.unwrap_err();
        let err = err.downcast_ref::<DiagramError>().unwrap();
        assert!(matches!(err, DiagramError::TypeMismatch(_)), "{:?}", err);
    }

    #[test]
    fn test_diagram_disconnected() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert!(matches!(
            *promise.take().cancellation().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_fork_clone() {
        let mut context = TestingContext::minimal_plugins();
        let mut registry = new_registry_with_basic_nodes();

        let bp = Diagram {
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
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        let err = result.unwrap_err();
        let err = err.downcast_ref::<DiagramError>().unwrap();
        assert!(matches!(err, DiagramError::NotCloneable), "{:?}", err);
    }

    #[test]
    fn test_run_fork_clone() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let bp = Diagram {
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
                        next: "op_2".to_string(),
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::ForkClone(ForkCloneOp {
                        next: vec!["op_3".to_string()],
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3_cloneable".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap().unwrap(), 12);
    }

    #[test]
    fn test_run_unzip() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        fn multiply3_5(x: i64) -> (i64, i64) {
            (x * 3, x * 5)
        }

        registry.register_node(
            "multiply3_5",
            "multiply3_5",
            (|builder: &mut Builder| builder.create_map_block(multiply3_5))
                .into_registration_builder()
                .with_unzippable(),
        );

        let bp = Diagram {
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
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["op_2".to_string(), "op_3".to_string()],
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap().unwrap(), 12);
    }

    #[test]
    fn test_run_serialized_diagram() {
        let mut registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let json_str = r#"
        {
            "start": {
                "type": "start",
                "next": "multiply3"
            },
            "multiply3": {
                "type": "node",
                "nodeId": "multiply3",
                "next": "terminate"
            },
            "terminate": {
                "type": "terminate"
            }
        }
        "#;

        let bp = Diagram::from_json_str(json_str).unwrap();
        let w = bp.spawn_io_workflow(&mut context.app, &mut registry);
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap().unwrap(), 12);
    }
}
