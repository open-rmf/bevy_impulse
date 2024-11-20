use std::collections::{HashMap, HashSet};

use log::debug;

use crate::{Builder, InputSlot, Output, StreamPack};

use super::{
    Diagram, DiagramError, DiagramOperation, DynInputSlot, DynNode, DynOutput, DynScope, NodeOp,
    NodeRegistration, NodeRegistry, OperationId, ScopeStart, ScopeTerminate, StartOp, TypeMismatch,
};

trait ConnectionChainOps {
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

    fn map_input(
        &self,
        builder: &mut Builder,
        input: DynInputSlot,
        registration: &NodeRegistration,
    ) -> Result<DynInputSlot, DiagramError>;

    fn terminate_sender(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<ScopeTerminate>, DiagramError>;
}

struct NodeConnectionChainOps<'a> {
    registration: &'a NodeRegistration,
}

impl<'a> NodeConnectionChainOps<'a> {
    fn new(registration: &'a NodeRegistration) -> Self {
        Self { registration }
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

    fn map_input(
        &self,
        _builder: &mut Builder,
        input: DynInputSlot,
        _registration: &NodeRegistration,
    ) -> Result<DynInputSlot, DiagramError> {
        Ok(input)
    }

    fn terminate_sender(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<ScopeTerminate>, DiagramError> {
        let sender = self.registration.create_sender(builder)?;
        WorkflowBuilder::connect_output(builder, output, sender.input)?;
        Ok(sender.output.into_output())
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

    fn unzip(
        &self,
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotUnzippable)
    }

    fn map_input(
        &self,
        builder: &mut Builder,
        input: DynInputSlot,
        registration: &NodeRegistration,
    ) -> Result<DynInputSlot, DiagramError> {
        let receiver = registration.create_receiver(builder)?;
        WorkflowBuilder::connect_output(builder, receiver.output, input)?;
        Ok(receiver.input)
    }

    fn terminate_sender(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Output<ScopeTerminate>, DiagramError> {
        let passthrough = builder.create_map_block(|v: ScopeStart| -> ScopeTerminate { Ok(v) });
        WorkflowBuilder::connect_output(builder, output, passthrough.input.into())?;
        Ok(passthrough.output)
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
                Ok((op_id, r.create_node(builder)))
            })
            .collect::<Result<_, _>>()?;
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
        if output.type_name != input.type_name {
            Err(DiagramError::TypeMismatch(TypeMismatch {
                output_type: output.type_name.to_string(),
                input_type: input.type_name.to_string(),
            }))
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
                chain_ops: NodeConnectionChainOps::new(source_registration),
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
            can_terminate: Option<DiagramError>,
        }

        let mut to_visit = vec![State {
            op_id: next_op_id,
            output,
            can_terminate: None,
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
                    let input = ops.map_input(builder, self.inputs[state.op_id], reg)?;
                    Self::connect_output(builder, state.output, input)?;
                }
                DiagramOperation::Terminate(_) => {
                    if let Some(err) = state.can_terminate {
                        return Err(err);
                    }
                    let sender = ops.terminate_sender(builder, state.output)?;
                    Self::connect_output(builder, sender.into(), terminate.into())?;
                }
                DiagramOperation::ForkClone(fork_clone_op) => {
                    debug!("fork_clone to {:?}", fork_clone_op.next);
                    let outputs =
                        ops.fork_clone(builder, state.output, fork_clone_op.next.len())?;
                    to_visit.extend(fork_clone_op.next.iter().zip(outputs).map(
                        |(next_op_id, output)| State {
                            op_id: next_op_id,
                            output,
                            can_terminate: None,
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
                            can_terminate: Some(DiagramError::NotUnzippable),
                        },
                    ));
                }
            }
        }

        Ok(())
    }
}
