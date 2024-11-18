use std::collections::HashMap;

use crate::{diagram::ScopeStart, Builder, StreamPack};

use super::{
    Diagram, DiagramError, DiagramOperation, DynInputSlot, DynNode, DynScope, ForkCloneOp, NodeOp,
    NodeRegistry, OperationId, ScopeTerminate, StartOp, TypeMismatch, UnzipOp,
};

pub struct WorkflowBuilder<'b, 'w, 's, 'a> {
    builder: &'b mut Builder<'w, 's, 'a>,
    registry: &'b mut NodeRegistry,
    diagram: &'b Diagram,
    inputs: HashMap<&'b OperationId, DynInputSlot>,
}

impl<'b, 'w, 's, 'a> WorkflowBuilder<'b, 'w, 's, 'a> {
    pub(super) fn new<Streams: StreamPack>(
        scope: &DynScope<Streams>,
        builder: &'b mut Builder<'w, 's, 'a>,
        registry: &'b mut NodeRegistry,
        diagram: &'b Diagram,
    ) -> Result<Self, DiagramError> {
        // nodes and outputs cannot be cloned, but input can be cloned, so we
        // first create all the nodes, store them in a map, then store the inputs
        // in another map. `connect` takes ownership of input and output so we must pop/take
        // from the nodes map but we can borrow and clone the inputs.
        let nodes = diagram
            .ops
            .iter()
            .filter_map(|(op_id, op)| match op {
                DiagramOperation::Node(node_op) => {
                    Some(registry.create_node(&node_op.node_id, builder).map_or_else(
                        || Err(DiagramError::NodeNotFound(node_op.node_id.clone())),
                        |v| Ok((op_id, v)),
                    ))
                }
                _ => None,
            })
            .collect::<Result<HashMap<&OperationId, DynNode>, _>>()?;
        let terminate = diagram
            .ops
            .iter()
            .find(|v| matches!(v.1, DiagramOperation::Terminate(_)))
            .into_iter()
            .map(|(terminate_op_id, _)| (terminate_op_id, DynInputSlot::from(scope.terminate)));
        let inputs: HashMap<&OperationId, DynInputSlot> = nodes
            .iter()
            .map(|(k, v)| (*k, v.input.clone()))
            .chain(terminate)
            .collect();

        Ok(Self {
            builder,
            registry,
            diagram,
            inputs,
        })
    }

    fn get_op(&self, op_id: &OperationId) -> Result<&DiagramOperation, DiagramError> {
        self.diagram
            .get_op(op_id)
            .map_err(|_| DiagramError::OperationNotFound(op_id.clone()))
    }

    fn validate_connection(&self, target_op: &DiagramOperation) -> Result<(), DiagramError> {
        match target_op {
            DiagramOperation::ForkClone(ForkCloneOp { next })
            | DiagramOperation::Unzip(UnzipOp { next }) => {
                for op_id in next {
                    let op = self.get_op(op_id)?;
                    if matches!(op, DiagramOperation::Start(_)) {
                        return Err(DiagramError::CannotConnectStart);
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn connect_node<Streams: StreamPack>(
        &mut self,
        scope: &DynScope<Streams>,
        op: &NodeOp,
    ) -> Result<(), DiagramError> {
        let target_op_id = &op.next;
        let source_metadata = &self
            .registry
            .get_registration(&op.node_id)
            .ok_or_else(|| DiagramError::NodeNotFound(op.node_id.clone()))?
            .metadata
            .clone();
        let source_node = self
            .registry
            .create_node(&op.node_id, self.builder)
            .ok_or(DiagramError::NodeNotFound(op.node_id.clone()))?;
        let target_op = self.get_op(&target_op_id)?;

        self.validate_connection(target_op)?;

        match target_op {
            DiagramOperation::Start(_) => Err(DiagramError::CannotConnectStart),
            DiagramOperation::Node(target_op) => {
                let target_metadata = &self
                    .registry
                    .get_registration(&target_op.node_id)
                    .ok_or(DiagramError::NodeNotFound(target_op.node_id.clone()))?
                    .metadata;
                let output_type = &source_metadata.response.r#type;
                let input_type = &target_metadata.request.r#type;

                if output_type != input_type {
                    return Err(DiagramError::TypeMismatch(TypeMismatch {
                        output_type: output_type.clone(),
                        input_type: input_type.clone(),
                    }));
                }

                let next_input = self.inputs.get(&target_op_id).unwrap();
                self.builder.connect(
                    source_node.output.into_output::<()>(),
                    next_input.into_input::<()>(),
                );
                Ok(())
            }
            DiagramOperation::Terminate(_) => {
                let sender = self
                    .registry
                    .create_sender(&op.node_id, self.builder)
                    .ok_or(DiagramError::NotSerializable)?;
                self.builder.connect(
                    sender.output.into_output::<ScopeTerminate>(),
                    scope.terminate,
                );
                self.builder.connect(
                    source_node.output.into_output::<()>(),
                    sender.input.into_input::<()>(),
                );
                Ok(())
            }
            DiagramOperation::ForkClone(next_op) => {
                if !source_metadata.response.cloneable {
                    return Err(DiagramError::NotCloneable);
                }
                let inputs = next_op
                    .next
                    .iter()
                    .map(|op_id| self.inputs.get(op_id).unwrap().clone())
                    .collect::<Vec<DynInputSlot>>();
                self.registry
                    .fork_clone(&op.node_id, self.builder, source_node.output, inputs)
                    .map_err(|_| DiagramError::NotCloneable)?;
                Ok(())
            }
            DiagramOperation::Unzip(next_op) => {
                if !source_metadata.response.unzip_slots != next_op.next.len() {
                    return Err(DiagramError::NotUnzippable);
                }
                let inputs = next_op
                    .next
                    .iter()
                    .map(|op_id| self.inputs.get(op_id).unwrap().clone())
                    .collect::<Vec<DynInputSlot>>();
                self.registry
                    .unzip(&op.node_id, self.builder, source_node.output, inputs)
                    .map_err(|_| DiagramError::NotUnzippable)?;
                Ok(())
            }
        }
    }

    pub(super) fn connect_start<Streams: StreamPack>(
        &mut self,
        scope: DynScope<Streams>,
        start_op: &StartOp,
    ) -> Result<(), DiagramError> {
        // using macro instead of closure because we can't mutable borrow with closures
        macro_rules! get_input {
            ($target_op_id: expr, $target_op: expr) => {
                match $target_op {
                    DiagramOperation::Start(_) => Err(DiagramError::CannotConnectStart),
                    DiagramOperation::Node(op) => {
                        let receiver = self
                            .registry
                            .create_receiver(&op.node_id, self.builder)
                            .ok_or(DiagramError::NotSerializable)?;
                        let target_input = self.inputs.get($target_op_id).unwrap();
                        self.builder.connect(
                            receiver.output.into_output::<()>(),
                            target_input.into_input::<()>(),
                        );
                        Ok(receiver.input)
                    }
                    DiagramOperation::Terminate(_) => Ok(scope.terminate.into()),
                    DiagramOperation::ForkClone(_) | DiagramOperation::Unzip(_) => {
                        Err(DiagramError::BadInterconnectChain)
                    }
                }
            };
        }

        let target_op_id = &start_op.next;
        let target_op = self.get_op(target_op_id)?.clone();
        self.validate_connection(&target_op)?;

        match target_op {
            DiagramOperation::Start(_) => Err(DiagramError::CannotConnectStart),
            DiagramOperation::Node(_) => {
                let input = get_input!(target_op_id, target_op)?;
                self.builder.connect(scope.input, input.into_input());
                Ok(())
            }
            DiagramOperation::Terminate(_) => {
                let passthrough = self
                    .builder
                    .create_map_block(|v: ScopeStart| -> ScopeTerminate { Ok(v) });
                self.builder.connect(scope.input, passthrough.input);
                self.builder.connect(passthrough.output, scope.terminate);
                Ok(())
            }
            DiagramOperation::ForkClone(op) => {
                let fc = scope.input.fork_clone(self.builder);
                for target_op_id in &op.next {
                    let target_op = self.get_op(target_op_id)?.clone();
                    let input = get_input!(target_op_id, target_op)?;
                    let output = fc.clone_output(self.builder);
                    self.builder.connect(output, input.into_input());
                }
                Ok(())
            }
            DiagramOperation::Unzip(_) => Err(DiagramError::NotUnzippable),
        }
    }
}
