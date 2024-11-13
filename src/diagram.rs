mod node_registry;
mod serialization;

pub use node_registry::*;
pub use serialization::*;

// ----------

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Display,
    io::Read,
};

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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForkCloneOp {
    next: Vec<OperationId>,
}

#[derive(Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DiagramOperation {
    Start(StartOp),
    Terminate(TerminateOp),
    Node(NodeOp),
    ForkClone(ForkCloneOp),
}

#[derive(Debug)]
struct ValidatedDiagram {
    start: StartOp,
    nodes: HashMap<OperationId, NodeOp>,
}

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
    ) -> Result<Service<serde_json::Value, serde_json::Value, Streams>, DiagramError>
    where
        Streams: StreamPack,
    {
        let diagram = self.validate(registry)?;

        let w = app.world.spawn_workflow(
            |scope: Scope<serde_json::Value, serde_json::Value, Streams>, builder: &mut Builder| {
                let mut dyn_builder = DynWorkflowBuilder { builder };

                // nodes and outputs cannot be cloned, but input can be cloned, so we
                // first create all the nodes, store them in a map, then store the inputs
                // in another map. `connect` takes ownership of input and output so we must pop/take
                // from the nodes map but we can borrow and clone the inputs.
                let mut nodes: HashMap<&OperationId, DynNode> = diagram
                    .nodes
                    .iter()
                    .filter_map(|(op_id, op)| {
                        Some((
                            op_id,
                            registry
                                .create_node(&op.node_id, dyn_builder.builder)
                                .unwrap(),
                        ))
                    })
                    .collect();
                let node_inputs: HashMap<&OperationId, DynInputSlot> =
                    nodes.iter().map(|(k, v)| (*k, v.input.clone())).collect();

                let get_fork_clone_inputs = |fork_clone_op: &ForkCloneOp| {
                    fork_clone_op
                        .next
                        .iter()
                        .map(|next_op_id| node_inputs.get(next_op_id).unwrap().clone())
                        .collect::<Vec<DynInputSlot>>()
                };

                // connect the start operation
                let next_op_id = diagram.start.next;
                let next_op = self.get_op(&next_op_id).unwrap();
                let next_inputs: Vec<DynInputSlot> = match &next_op {
                    DiagramOperation::Node(next_op) => {
                        let target_node_id = &next_op.node_id;
                        let receiver = registry
                            .create_receiver(target_node_id, dyn_builder.builder)
                            .unwrap();
                        let target_input = node_inputs.get(&next_op_id).unwrap();
                        dyn_builder.connect(receiver.output, target_input.clone());
                        vec![receiver.input]
                    }
                    DiagramOperation::Terminate(_) => vec![scope.terminate.into()],
                    DiagramOperation::ForkClone(next_op) => get_fork_clone_inputs(next_op),
                    _ => panic!("invalid connection"),
                };
                if next_inputs.len() == 1 {
                    dyn_builder.connect(scope.input.into(), next_inputs.get(0).unwrap().clone())
                } else {
                    dyn_builder.connect_fork_clone(scope.input.into(), next_inputs)
                }

                // connect node operations
                for (source_op_id, source_op) in &diagram.nodes {
                    let source_node = nodes.remove(&source_op_id).unwrap();
                    let next_op = self.ops.get(&source_op.next).unwrap();

                    let next_inputs: Vec<DynInputSlot> = match &next_op {
                        DiagramOperation::Node(_) => {
                            let next_node = nodes.get(&source_op.next).unwrap();
                            vec![next_node.input.clone()]
                        }
                        DiagramOperation::Terminate(_) => {
                            let sender = registry
                                .create_sender(&source_op.node_id, dyn_builder.builder)
                                .unwrap();
                            dyn_builder.connect(sender.output, scope.terminate.into());
                            vec![sender.input]
                        }
                        DiagramOperation::ForkClone(next_op) => get_fork_clone_inputs(next_op),
                        _ => panic!("invalid connection"),
                    };

                    if next_inputs.len() == 1 {
                        dyn_builder.connect(source_node.output, next_inputs.get(0).unwrap().clone())
                    } else {
                        dyn_builder.connect_fork_clone(source_node.output, next_inputs)
                    }
                }
            },
        );

        Ok(w)
    }

    pub fn spawn_io_workflow(
        &self,
        app: &mut App,
        registry: &mut NodeRegistry,
    ) -> Result<Service<serde_json::Value, serde_json::Value>, DiagramError> {
        self.spawn_workflow::<()>(app, registry)
    }

    pub fn from_json_str(s: &str) -> Result<Self, DiagramError> {
        Ok(serde_json::from_str(s)?)
    }

    pub fn from_reader<R>(r: R) -> Result<Self, DiagramError>
    where
        R: Read,
    {
        Ok(serde_json::from_reader(r)?)
    }

    /// Checks that the workflow is valid, returns the start and terminate vertices.
    ///
    /// More formally, it checks that
    /// 1. All nodes used are registered.
    /// 2. There is a path from start to terminate.
    /// 3. The terminate vertex is no edges to other vertices.
    /// 4. There are no edges into the start vertex.
    /// 5. All the connections are valid (their request and response type matches).
    /// 6. There can be multiple outgoing edges only if the response is cloneable.
    fn validate(&self, registry: &NodeRegistry) -> Result<ValidatedDiagram, DiagramError> {
        let mut start: Option<(&OperationId, StartOp)> = None;
        let mut terminate: Option<(&OperationId, TerminateOp)> = None;
        let mut nodes: HashMap<OperationId, NodeOp> = HashMap::new();

        for (op_id, op) in self.ops.iter() {
            macro_rules! validation_error {
                ($err:expr) => {
                    DiagramError::from_validation_error(op_id.clone(), $err)
                };
            }

            match op {
                DiagramOperation::Start(op) => {
                    if start.is_some() {
                        return Err(validation_error!(ValidationError::MultipleStartTerminate));
                    }
                    self.validate_start(op, registry)
                        .map_err(|err| validation_error!(err))?;
                    start = Some((op_id, op.clone()));
                }
                DiagramOperation::Terminate(op) => {
                    if terminate.is_some() {
                        return Err(validation_error!(ValidationError::MultipleStartTerminate));
                    }
                    self.validate_terminate(op)?;
                    terminate = Some((op_id, op.clone()));
                }
                DiagramOperation::Node(op) => {
                    self.validate_node(op, registry)
                        .map_err(|err| validation_error!(err))?;
                    nodes.insert(op_id.clone(), op.clone());
                }
                DiagramOperation::ForkClone(op) => {
                    self.validate_fork_clone(op)
                        .map_err(|err| validation_error!(err))?;
                }
            }
        }

        if let (Some((start_id, start)), Some((terminate_id, _terminate))) = (start, terminate) {
            self.check_connected(start_id, terminate_id)?;
            return Ok(ValidatedDiagram { start, nodes });
        }
        Err(DiagramError::DisconnectedGraph)
    }

    /// Checks if there is a path from between 2 operations
    fn check_connected(
        &self,
        from_id: &OperationId,
        to_id: &OperationId,
    ) -> Result<(), DiagramError> {
        let mut visited: HashSet<&OperationId> = HashSet::new();
        let mut search: Vec<&OperationId> = vec![&from_id];

        while let Some(op_id) = search.pop() {
            let op = self
                .get_op(op_id)
                .map_err(|err| DiagramError::from_validation_error(op_id.clone(), err))?;
            if op_id == to_id {
                return Ok(());
            }

            visited.insert(&op_id);
            let next_op_ids: Vec<&String> = match &op {
                DiagramOperation::Start(op) => vec![&op.next],
                DiagramOperation::Node(op) => vec![&op.next],
                _ => vec![],
            };
            for next_op_id in next_op_ids {
                if visited.insert(&next_op_id) {
                    search.push(&next_op_id);
                }
            }
        }

        Err(DiagramError::DisconnectedGraph)
    }

    fn get_op(&self, op_id: &OperationId) -> Result<&DiagramOperation, ValidationError> {
        self.ops
            .get(op_id)
            .ok_or_else(|| ValidationError::OperationNotFound(op_id.clone()))
    }

    fn get_node_registration<'a>(
        id: &NodeId,
        registry: &'a NodeRegistry,
    ) -> Result<&'a NodeRegistration, ValidationError> {
        registry
            .get_registration(id)
            .ok_or_else(|| ValidationError::NodeNotFound(id.clone()))
    }

    fn validate_start(
        &self,
        start_op: &StartOp,
        registry: &NodeRegistry,
    ) -> Result<(), ValidationError> {
        let target_op = self.get_op(&start_op.next)?;
        match &target_op {
            DiagramOperation::Node(node_op) => {
                let target_node = Self::get_node_registration(&node_op.node_id, registry)?;
                if !target_node.metadata.request.deserializable {
                    return Err(ValidationError::UnserializableOperation);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_terminate(&self, _terminate_op: &TerminateOp) -> Result<(), DiagramError> {
        Ok(())
    }

    fn validate_node(
        &self,
        node_op: &NodeOp,
        registry: &NodeRegistry,
    ) -> Result<(), ValidationError> {
        let node = Self::get_node_registration(&node_op.node_id, registry)?;

        let next_op = self.get_op(&node_op.next)?;
        match &next_op {
            DiagramOperation::Start(_) => return Err(ValidationError::InvalidStartOperation),
            DiagramOperation::Terminate(_) => {
                if !node.metadata.response.serializable {
                    return Err(ValidationError::UnserializableOperation);
                }
            }
            DiagramOperation::Node(next_op) => {
                let next_node = Self::get_node_registration(&next_op.node_id, registry)?;
                let output_type = &node.metadata.response.r#type;
                let input_type = &next_node.metadata.request.r#type;
                if output_type != input_type {
                    return Err(ValidationError::TypeMismatch(TypeMismatch {
                        output_type: output_type.clone(),
                        input_type: input_type.clone(),
                    }));
                }
            }
            DiagramOperation::ForkClone(_) => {
                if !node.metadata.response.cloneable {
                    return Err(ValidationError::NotCloneable);
                }
            }
        }
        Ok(())
    }

    fn validate_fork_clone(&self, fork_clone_op: &ForkCloneOp) -> Result<(), ValidationError> {
        for next_op_id in &fork_clone_op.next {
            let next_op = self.get_op(next_op_id)?;
            match next_op {
                DiagramOperation::Start(_) => return Err(ValidationError::InvalidStartOperation),
                _ => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TypeMismatch {
    output_type: String,
    input_type: String,
}

#[derive(Debug)]
pub enum ValidationError {
    TypeMismatch(TypeMismatch),
    NodeNotFound(NodeId),
    OperationNotFound(OperationId),
    MultipleStartTerminate,
    InvalidStartOperation,
    UnserializableOperation,
    NotCloneable,
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeMismatch(data) => write!(
                f,
                "output type [{}] does not match input type [{}]",
                data.output_type, data.input_type
            ),
            Self::NodeNotFound(node_id) => {
                write!(f, "node [{}] not is not registered", node_id)
            }
            Self::OperationNotFound(op_id) => {
                write!(f, "operation [{}] not found", op_id)
            }
            Self::MultipleStartTerminate => {
                f.write_str("there must be exactly one start and terminate")
            }
            Self::InvalidStartOperation => f.write_str("start operation cannot receive inputs"),
            Self::UnserializableOperation => {
                f.write_str("request or response cannot be serialized")
            }
            Self::NotCloneable => f.write_str("response cannot be cloned"),
        }
    }
}

impl Error for ValidationError {}

#[derive(Debug)]
pub enum DiagramError {
    DisconnectedGraph,
    ValidationError(OperationId, ValidationError),
    JsonError(serde_json::Error),
}

impl DiagramError {
    fn from_validation_error(op_id: OperationId, op_err: ValidationError) -> Self {
        DiagramError::ValidationError(op_id, op_err)
    }
}

impl Display for DiagramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DisconnectedGraph => f.write_str("No path reaches terminate"),
            Self::ValidationError(op_id, err) => {
                write!(f, "error in operation [{}]: {}", op_id, err)
            }
            Self::JsonError(err) => err.fmt(f),
        }
    }
}

impl Error for DiagramError {}

impl From<serde_json::Error> for DiagramError {
    fn from(json_err: serde_json::Error) -> Self {
        Self::JsonError(json_err)
    }
}

pub struct DynWorkflowBuilder<'b, 'w, 's, 'a> {
    builder: &'b mut Builder<'w, 's, 'a>,
}

impl<'b, 'w, 's, 'a> DynWorkflowBuilder<'b, 'w, 's, 'a> {
    /// Connect a [`DynOutput`] to a [`DynInputSlot`], note that this DOES NOT do any validation
    /// checks, if the slots are not compatible this will create an invalid workflow!
    fn connect(&mut self, output: DynOutput, input: DynInputSlot) {
        self.builder
            .connect(output.into_output::<()>(), input.into_input::<()>());
    }

    /// Connect a [`DynOutput`] to multiple [`DynInputSlot`], note that this DOES NOT do any validation
    /// checks, if the slots are not compatible this will create an invalid workflow!
    fn connect_fork_clone<InputIter>(&mut self, output: DynOutput, inputs: InputIter)
    where
        InputIter: IntoIterator<Item = DynInputSlot>,
    {
        let fork = output.into_output::<()>().fork_clone(self.builder);
        for input in inputs {
            let cloned_output = fork.clone_output(self.builder);
            self.connect(cloned_output.into(), input);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{testing::TestingContext, RequestExt};

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
        registry.register_node("multiply3", "multiply3", |builder| {
            builder.create_map_block(multiply3)
        });
        registry.register_node("multiply3_cloneable", "multiply3_cloneable", |builder| {
            builder
                .create_map_block(multiply3)
                .into_registration_builder()
                .with_response_cloneable()
        });
        registry.register_node("opaque", "opaque", |builder| {
            builder
                .create_map_block(opaque)
                .into_registration_builder()
                .with_opaque_request()
                .with_opaque_response()
        });
        registry.register_node("opaque_request", "opaque_request", |builder| {
            builder
                .create_map_block(opaque_request)
                .into_registration_builder()
                .with_opaque_request()
        });
        registry.register_node("opaque_response", "opaque_response", |builder| {
            builder
                .create_map_block(opaque_response)
                .into_registration_builder()
                .with_opaque_response()
        });
        registry
    }

    #[test]
    fn test_validate_diagram_no_terminate() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(matches!(err, DiagramError::DisconnectedGraph), "{:?}", err);
    }

    #[test]
    fn test_validate_diagram_no_start() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(matches!(err, DiagramError::DisconnectedGraph), "{:?}", err);
    }

    #[test]
    fn test_validate_diagram_connect_to_start() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(
            matches!(
                err,
                DiagramError::ValidationError(_, ValidationError::InvalidStartOperation)
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_validate_diagram_unserializable_start() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(
            matches!(
                err,
                DiagramError::ValidationError(_, ValidationError::UnserializableOperation)
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_validate_diagram_unserializable_terminate() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(
            matches!(
                err,
                DiagramError::ValidationError(_, ValidationError::UnserializableOperation)
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_validate_diagram_mismatch_types() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(
            matches!(
                err,
                DiagramError::ValidationError(_, ValidationError::TypeMismatch(_))
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_validate_diagram_disconnected() {
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

        let registry = new_registry_with_basic_nodes();
        let err = bp.validate(&registry).unwrap_err();
        assert!(matches!(err, DiagramError::DisconnectedGraph), "{:?}", err);
    }

    #[test]
    fn test_run_diagram() {
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

        let workflow = bp
            .spawn_io_workflow(&mut context.app, &mut registry)
            .unwrap();

        let mut promise = context.command(|cmds| {
            cmds.request(serde_json::Value::from(4), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 12);
    }

    #[test]
    fn test_validate_fork_clone() {
        let registry = new_registry_with_basic_nodes();

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

        let err = bp.validate(&registry).unwrap_err();
        assert!(
            matches!(
                err,
                DiagramError::ValidationError(_, ValidationError::NotCloneable)
            ),
            "{:?}",
            err
        );
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
        let workflow = bp
            .spawn_io_workflow(&mut context.app, &mut registry)
            .unwrap();

        let mut promise = context.command(|cmds| {
            cmds.request(serde_json::Value::from(4), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 12);
    }
}
