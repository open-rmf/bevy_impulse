use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

use crate::{Builder, InputSlot, Output, StreamPack};

use super::{
    split_chain, transform::transform_output, Diagram, DiagramError, DiagramOperation,
    DynInputSlot, DynNode, DynOutput, DynScope, DynSplitOutputs, NodeOp, NodeRegistration,
    NodeRegistry, OperationId, ScopeStart, ScopeTerminate, SplitOp, StartOp,
};

trait OutputOperations {
    fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError>;

    fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError>;

    fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError>;

    fn split<'a>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError>;

    fn deserialize(
        &self,
        builder: &mut Builder,
        output: Output<serde_json::Value>,
        input_type: TypeId,
        registry: &NodeRegistry,
    ) -> Result<DynOutput, DiagramError>;

    fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        registry: &NodeRegistry,
    ) -> Result<Output<serde_json::Value>, DiagramError>;
}

impl OutputOperations for &NodeRegistration {
    fn fork_clone(
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

    fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        let f = self
            .unzip_impl
            .as_ref()
            .ok_or(DiagramError::NotUnzippable)?;
        f(builder, output)
    }

    fn fork_result(
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

    fn split<'a>(
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

    fn deserialize(
        &self,
        builder: &mut Builder,
        output: Output<serde_json::Value>,
        input_type: TypeId,
        registry: &NodeRegistry,
    ) -> Result<DynOutput, DiagramError> {
        if !self.metadata.request.deserializable {
            Err(DiagramError::NotSerializable)
        } else {
            let deserialize = &registry.deserialize_impls[&input_type];
            Ok(deserialize(builder, output))
        }
    }

    fn serialize(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        registry: &NodeRegistry,
    ) -> Result<Output<serde_json::Value>, DiagramError> {
        if output.type_id == TypeId::of::<serde_json::Value>() {
            Ok(output.into_output())
        } else if !self.metadata.response.serializable {
            Err(DiagramError::NotSerializable)
        } else {
            let serialize = &registry.serialize_impls[&output.type_id];
            Ok(serialize(builder, output))
        }
    }
}

struct JsonOutputOperations;

impl OutputOperations for JsonOutputOperations {
    fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        let o = output.into_output::<ScopeStart>();
        let fork_clone = o.fork_clone(builder);
        Ok((0..amount)
            .map(|_| fork_clone.clone_output(builder).into())
            .collect())
    }

    fn unzip(
        &self,
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotUnzippable)
    }

    fn fork_result(
        &self,
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        Err(DiagramError::CannotForkResult)
    }

    fn split<'a>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError> {
        let chain = output.into_output::<serde_json::Value>().chain(builder);
        split_chain(chain, split_op)
    }

    fn deserialize(
        &self,
        _builder: &mut Builder,
        output: Output<serde_json::Value>,
        _input_type: TypeId,
        _registry: &NodeRegistry,
    ) -> Result<DynOutput, DiagramError> {
        Ok(output.into())
    }

    fn serialize(
        &self,
        _builder: &mut Builder,
        output: DynOutput,
        _registry: &NodeRegistry,
    ) -> Result<Output<serde_json::Value>, DiagramError> {
        Ok(output.into_output())
    }
}

struct ConnectionSource<Ops> {
    output: DynOutput,
    ops: Ops,
}

pub struct WorkflowBuilder<'b> {
    registry: &'b NodeRegistry,
    diagram: &'b Diagram,
    nodes: HashMap<&'b OperationId, DynNode>,
    inputs: HashMap<&'b OperationId, DynInputSlot>,
}

impl<'b> WorkflowBuilder<'b> {
    pub(super) fn new<Streams: StreamPack>(
        scope: &DynScope<Streams>,
        builder: &mut Builder,
        registry: &'b NodeRegistry,
        diagram: &'b Diagram,
    ) -> Result<Self, DiagramError> {
        // nodes and outputs cannot be cloned, but input can be cloned, so we
        // first create all the nodes, store them in a map, then store the inputs
        // in another map. `connect` takes ownership of input and output so we must pop/take
        // from the nodes map but we can borrow and clone the inputs.
        let nodes: HashMap<&OperationId, DynNode> = diagram
            .ops
            .iter()
            .filter_map(|(op_id, op)| match op {
                DiagramOperation::Node(node_op) => Some((op_id, node_op)),
                _ => None,
            })
            .map(|(op_id, node_op)| {
                let r = registry.get_registration(&node_op.node_id)?;
                let n = r.create_node(builder, node_op.config.clone())?;
                Ok((op_id, n))
            })
            .collect::<Result<_, DiagramError>>()?;
        let terminate_input = diagram
            .ops
            .iter()
            .find(|(_, op)| matches!(op, DiagramOperation::Terminate(_)))
            .map(|(op_id, _)| (op_id, DynInputSlot::from(scope.terminate)));
        let inputs: HashMap<&OperationId, DynInputSlot> = nodes
            .iter()
            .map(|(k, v)| (*k, v.input))
            .chain(terminate_input)
            .collect();

        Ok(Self {
            registry,
            diagram,
            nodes,
            inputs,
        })
    }

    fn get_op(&self, op_id: &OperationId) -> Result<&'b DiagramOperation, DiagramError> {
        self.diagram
            .get_op(op_id)
            .map_err(|_| DiagramError::OperationNotFound(op_id.clone()))
    }

    fn dyn_connect(
        builder: &mut Builder,
        output: DynOutput,
        input: DynInputSlot,
    ) -> Result<(), DiagramError> {
        if output.type_id != input.type_id {
            Err(DiagramError::TypeMismatch)
        } else {
            builder.connect(output.into_output::<()>(), input.into_input::<()>());
            Ok(())
        }
    }

    pub(super) fn connect_node<Streams: StreamPack>(
        &mut self,
        scope: &DynScope<Streams>,
        builder: &mut Builder,
        op_id: &OperationId,
        op: &NodeOp,
    ) -> Result<(), DiagramError> {
        let target_op_id = &op.next;
        let source_registration = self.registry.get_registration(&op.node_id)?;
        let source_node = self
            .nodes
            .remove(op_id)
            .ok_or(DiagramError::OperationNotFound(op_id.clone()))?;

        self.connect_next(
            builder,
            ConnectionSource {
                output: source_node.output,
                ops: source_registration,
            },
            target_op_id,
            scope.terminate,
        )
    }

    pub(super) fn connect_start<Streams: StreamPack>(
        &self,
        scope: DynScope<Streams>,
        builder: &mut Builder,
        start_op: &StartOp,
    ) -> Result<(), DiagramError> {
        let target_op_id = &start_op.next;

        self.connect_next(
            builder,
            ConnectionSource {
                output: scope.input.into(),
                ops: JsonOutputOperations {},
            },
            target_op_id,
            scope.terminate,
        )
    }

    fn connect_next<Ops>(
        &self,
        builder: &mut Builder,
        source: ConnectionSource<Ops>,
        next_op_id: &OperationId,
        terminate: InputSlot<ScopeTerminate>,
    ) -> Result<(), DiagramError>
    where
        Ops: OutputOperations,
    {
        let ConnectionSource { output, ops } = source;

        struct State<'a> {
            op_id: &'a OperationId,
            output: DynOutput,
        }

        let mut to_visit = vec![State {
            op_id: next_op_id,
            output,
        }];
        let mut visited: HashSet<&OperationId> = HashSet::new();

        while let Some(state) = to_visit.pop() {
            if !visited.insert(state.op_id) {
                continue;
            }

            let op = self.get_op(state.op_id)?;
            match op {
                DiagramOperation::Start(_) => return Err(DiagramError::CannotConnectStart),
                DiagramOperation::Node(node_op) => {
                    let input = self.inputs[state.op_id];
                    let output = if state.output.type_id == TypeId::of::<serde_json::Value>() {
                        let reg = self.registry.get_registration(&node_op.node_id)?;
                        reg.deserialize(
                            builder,
                            state.output.into_output(),
                            input.type_id,
                            self.registry,
                        )?
                    } else {
                        state.output
                    };
                    Self::dyn_connect(builder, output, input)?;
                }
                DiagramOperation::Terminate(_) => {
                    let output = ops.serialize(builder, state.output, self.registry)?;
                    builder.connect(output, terminate);
                }
                DiagramOperation::ForkClone(fork_clone_op) => {
                    let outputs =
                        ops.fork_clone(builder, state.output, fork_clone_op.next.len())?;
                    to_visit.extend(fork_clone_op.next.iter().zip(outputs).map(
                        |(next_op_id, output)| State {
                            op_id: next_op_id,
                            output,
                        },
                    ));
                }
                DiagramOperation::Unzip(unzip_op) => {
                    let outputs = ops.unzip(builder, state.output)?;
                    if outputs.len() < unzip_op.next.len() {
                        return Err(DiagramError::NotUnzippable);
                    }
                    to_visit.extend(unzip_op.next.iter().zip(outputs).map(
                        |(next_op_id, output)| State {
                            op_id: next_op_id,
                            output,
                        },
                    ));
                }
                DiagramOperation::ForkResult(fork_result_op) => {
                    let (ok_out, err_out) = ops.fork_result(builder, state.output)?;
                    to_visit.push(State {
                        op_id: &fork_result_op.ok,
                        output: ok_out,
                    });
                    to_visit.push(State {
                        op_id: &fork_result_op.err,
                        output: err_out,
                    });
                }
                DiagramOperation::Split(split_op) => {
                    let outputs = ops.split(builder, state.output, split_op)?;
                    for (op_id, output) in outputs.outputs {
                        to_visit.push(State {
                            op_id: &op_id,
                            output,
                        })
                    }

                    if let Some(remaining_op_id) = split_op.remaining.as_ref() {
                        to_visit.push(State {
                            op_id: remaining_op_id,
                            output: outputs.remaining,
                        });
                    }
                }
                DiagramOperation::Transform(transform_op) => {
                    let transformed_output =
                        transform_output(builder, self.registry, state.output, transform_op)?;
                    to_visit.push(State {
                        op_id: &transform_op.next,
                        output: transformed_output.into(),
                    });
                }
                DiagramOperation::Dispose => {}
            }
        }

        Ok(())
    }
}
