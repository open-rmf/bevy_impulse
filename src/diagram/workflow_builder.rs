use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
};

use log::debug;

use crate::{Builder, InputSlot, StreamPack};

use super::{
    Diagram, DiagramError, DiagramOperation, DynInputSlot, DynNode, DynOutput, DynScope,
    DynSplitOutputs, NodeOp, NodeRegistration, NodeRegistry, OperationId, ScopeStart,
    ScopeTerminate, SplitOpParams, StartOp,
};

#[allow(unused_variables)]
trait ConnectionChainOps {
    fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotCloneable)
    }

    fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotUnzippable)
    }

    fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        Err(DiagramError::CannotForkResult)
    }

    fn split<'a>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOpParams,
    ) -> Result<DynSplitOutputs<'a>, DiagramError> {
        Err(DiagramError::NotSplittable)
    }

    fn receiver(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        input_type: TypeId,
    ) -> Result<DynOutput, DiagramError> {
        Ok(output)
    }

    fn sender(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        input_type: TypeId,
    ) -> Result<DynOutput, DiagramError> {
        Ok(output)
    }
}

struct NodeConnectionChainOps<'a> {
    registry: &'a NodeRegistry,
    registration: &'a NodeRegistration,
}

impl<'a> NodeConnectionChainOps<'a> {
    fn new(registry: &'a NodeRegistry, registration: &'a NodeRegistration) -> Self {
        Self {
            registry,
            registration,
        }
    }
}

impl<'a> ConnectionChainOps for NodeConnectionChainOps<'a> {
    fn fork_clone(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        self.registration.fork_clone(builder, output, amount)
    }

    fn unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        self.registration.unzip(builder, output)
    }

    fn fork_result(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(DynOutput, DynOutput), DiagramError> {
        self.registration.fork_result(builder, output)
    }

    fn split<'b>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'b SplitOpParams,
    ) -> Result<DynSplitOutputs<'b>, DiagramError> {
        self.registration.split(builder, output, split_op)
    }

    fn receiver(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        input_type: TypeId,
    ) -> Result<DynOutput, DiagramError> {
        if output.type_id != TypeId::of::<serde_json::Value>() {
            return Ok(output);
        }
        // don't need to deserialize if input is also json
        if input_type == TypeId::of::<serde_json::Value>() {
            return Ok(output);
        }
        if !self.registration.metadata.request.deserializable {
            return Err(DiagramError::NotSerializable);
        }
        let deserialize = self
            .registry
            .deserialize_impls
            .get(&input_type)
            .ok_or(DiagramError::NotSerializable)?;
        Ok(deserialize(builder, output.into_output()))
    }

    fn sender(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        input_type: TypeId,
    ) -> Result<DynOutput, DiagramError> {
        if input_type != TypeId::of::<serde_json::Value>() {
            return Ok(output);
        }
        // don't need to serialize if output is already json
        if output.type_id == TypeId::of::<serde_json::Value>() {
            return Ok(output);
        }
        if !self.registration.metadata.response.serializable {
            return Err(DiagramError::NotSerializable);
        }
        let serialize = self
            .registry
            .serialize_impls
            .get(&output.type_id)
            .ok_or(DiagramError::NotSerializable)?;
        Ok(serialize(builder, output).into())
    }
}

struct StartConnectionChainOps;

impl ConnectionChainOps for StartConnectionChainOps {
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
}

struct ConnectionSource<T> {
    output: DynOutput,
    chain_ops: T,
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
        registry: &'b mut NodeRegistry,
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

    fn connect_output(
        builder: &mut Builder,
        output: DynOutput,
        input: DynInputSlot,
    ) -> Result<(), DiagramError> {
        debug!("connect [{:?}] to [{:?}]", output.id(), input.id());
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
                chain_ops: NodeConnectionChainOps::new(&self.registry, source_registration),
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
                chain_ops: StartConnectionChainOps {},
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
        Ops: ConnectionChainOps,
    {
        let ConnectionSource {
            output,
            chain_ops: ops,
        } = source;

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
                    let reg = self.registry.get_registration(&node_op.node_id)?;
                    let node_conn = NodeConnectionChainOps::new(&self.registry, reg);
                    let input = self.inputs[state.op_id];
                    let mapped_output = ops.sender(builder, state.output, input.type_id)?;
                    let mapped_output =
                        node_conn.receiver(builder, mapped_output, input.type_id)?;
                    Self::connect_output(builder, mapped_output, input)?;
                }
                DiagramOperation::Terminate(_) => {
                    let mapped_output =
                        ops.sender(builder, state.output, TypeId::of::<ScopeTerminate>())?;
                    Self::connect_output(builder, mapped_output, terminate.into())?;
                }
                DiagramOperation::ForkClone(fork_clone_op) => {
                    debug!("fork_clone to {:?}", fork_clone_op.next);
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
                    debug!("unzip to {:?}", unzip_op.next);
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
                    debug!(
                        "fork result ok to {:?}, err to {:?}",
                        fork_result_op.ok, fork_result_op.err
                    );
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
                    debug!("split {:?}", split_op);
                    let outputs = ops.split(builder, state.output, &split_op.params)?;
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
                DiagramOperation::Dispose => {}
            }
        }

        Ok(())
    }
}
