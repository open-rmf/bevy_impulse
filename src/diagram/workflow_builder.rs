use std::{any::TypeId, collections::HashMap};

use tracing::{debug, warn};

use crate::{
    diagram::join::serialize_and_join, unknown_diagram_error, Builder, InputSlot, Output,
    StreamPack,
};

use super::{
    fork_clone::DynForkClone, impls::DefaultImpl, split_chain, transform::transform_output,
    BuiltinTarget, Diagram, DiagramError, DiagramOperation, DiagramScope, DynInputSlot, DynOutput,
    NextOperation, NodeOp, NodeRegistry, OperationId, SourceOperation,
};

struct Vertex<'a> {
    op_id: &'a OperationId,
    op: &'a DiagramOperation,
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
}

struct Edge<'a> {
    source: SourceOperation,
    target: &'a NextOperation,
    state: EdgeState<'a>,
}

enum EdgeState<'a> {
    Ready {
        output: DynOutput,
        /// The node that initially produces the output, may be `None` if there is no origin.
        /// e.g. The entry point, or if the output passes through a `join` operation which
        /// results in multiple origins.
        origin: Option<&'a NodeOp>,
    },
    Pending,
}

pub(super) fn create_workflow<'a, Streams: StreamPack>(
    scope: DiagramScope<Streams>,
    builder: &mut Builder,
    registry: &NodeRegistry,
    diagram: &'a Diagram,
) -> Result<(), DiagramError> {
    // first create all the vertices
    let mut vertices: HashMap<&OperationId, Vertex> = diagram
        .ops
        .iter()
        .map(|(op_id, op)| {
            (
                op_id,
                Vertex {
                    op_id,
                    op,
                    in_edges: Vec::new(),
                    out_edges: Vec::new(),
                },
            )
        })
        .collect();

    // init with some capacity to reduce resizing. HashMap for faster removal.
    // NOTE: There are many `unknown_diagram_errors!()` used when accessing this.
    // In theory these accesses should never fail because the keys come from
    // `vertices` which are built using the same data as `edges`. But we do modify
    // `edges` while we are building the workflow so if an unknown error occurs, it is
    // likely due to some logic issue in the algorithm.
    let mut edges: HashMap<usize, Edge> = HashMap::with_capacity(diagram.ops.len() * 2);

    // process start separately because we need to consume the scope input
    match &diagram.start {
        NextOperation::Builtin { builtin } => match builtin {
            BuiltinTarget::Terminate => {
                // such a workflow is equivalent to an no-op.
                builder.connect(scope.input, scope.terminate);
                return Ok(());
            }
            BuiltinTarget::Dispose => {
                // bevy_impulse will immediate stop with an `CancellationCause::Unreachable` error
                // if trying to run such a workflow.
                return Ok(());
            }
        },
        NextOperation::Target(op_id) => {
            edges.insert(
                edges.len(),
                Edge {
                    source: SourceOperation::Builtin {
                        builtin: super::BuiltinSource::Start,
                    },
                    target: &diagram.start,
                    state: EdgeState::Ready {
                        output: scope.input.into(),
                        origin: None,
                    },
                },
            );
            vertices
                .get_mut(&op_id)
                .ok_or(DiagramError::OperationNotFound(op_id.clone()))?
                .in_edges
                .push(0);
        }
    };

    let mut inputs: HashMap<&OperationId, DynInputSlot> = HashMap::with_capacity(diagram.ops.len());

    let mut terminate_edges: Vec<usize> = Vec::new();

    let mut add_edge = |source: SourceOperation,
                        target: &'a NextOperation,
                        state: EdgeState<'a>|
     -> Result<(), DiagramError> {
        let source_id = if let SourceOperation::Source(source) = &source {
            Some(source.clone())
        } else {
            None
        };

        edges.insert(
            edges.len(),
            Edge {
                source,
                target,
                state,
            },
        );
        let new_edge_id = edges.len() - 1;

        if let Some(source_id) = source_id {
            let source_vertex = vertices
                .get_mut(&source_id)
                .ok_or_else(|| DiagramError::OperationNotFound(source_id.clone()))?;
            source_vertex.out_edges.push(new_edge_id);
        }

        match target {
            NextOperation::Target(target) => {
                let target_vertex = vertices
                    .get_mut(target)
                    .ok_or_else(|| DiagramError::OperationNotFound(target.clone()))?;
                target_vertex.in_edges.push(new_edge_id);
            }
            NextOperation::Builtin { builtin } => match builtin {
                BuiltinTarget::Terminate => {
                    terminate_edges.push(new_edge_id);
                }
                BuiltinTarget::Dispose => {}
            },
        }
        Ok(())
    };

    // create all the edges
    for (op_id, op) in &diagram.ops {
        match op {
            DiagramOperation::Node(node_op) => {
                let reg = registry.get_registration(&node_op.builder)?;
                let n = reg.create_node(builder, node_op.config.clone())?;
                inputs.insert(op_id, n.input);
                add_edge(
                    op_id.clone().into(),
                    &node_op.next,
                    EdgeState::Ready {
                        output: n.output.into(),
                        origin: Some(node_op),
                    },
                )?;
            }
            DiagramOperation::ForkClone(fork_clone_op) => {
                for next_op_id in fork_clone_op.next.iter() {
                    add_edge(op_id.clone().into(), next_op_id, EdgeState::Pending)?;
                }
            }
            DiagramOperation::Unzip(unzip_op) => {
                for next_op_id in unzip_op.next.iter() {
                    add_edge(op_id.clone().into(), next_op_id, EdgeState::Pending)?;
                }
            }
            DiagramOperation::ForkResult(fork_result_op) => {
                add_edge(op_id.clone().into(), &fork_result_op.ok, EdgeState::Pending)?;
                add_edge(
                    op_id.clone().into(),
                    &fork_result_op.err,
                    EdgeState::Pending,
                )?;
            }
            DiagramOperation::Split(split_op) => {
                let next_op_ids: Vec<&NextOperation> = split_op
                    .sequential
                    .iter()
                    .chain(split_op.keyed.values())
                    .collect();
                for next_op_id in next_op_ids {
                    add_edge(op_id.clone().into(), next_op_id, EdgeState::Pending)?;
                }
                if let Some(remaining) = &split_op.remaining {
                    add_edge(op_id.clone().into(), &remaining, EdgeState::Pending)?;
                }
            }
            DiagramOperation::Join(join_op) => {
                add_edge(op_id.clone().into(), &join_op.next, EdgeState::Pending)?;
            }
            DiagramOperation::Transform(transform_op) => {
                add_edge(op_id.clone().into(), &transform_op.next, EdgeState::Pending)?;
            }
            DiagramOperation::Dispose => {}
        }
    }

    let mut unconnected_vertices: Vec<&Vertex> = vertices.values().collect();
    while unconnected_vertices.len() > 0 {
        let ws = unconnected_vertices.clone();
        let ws_length = ws.len();
        unconnected_vertices.clear();

        for v in ws {
            let in_edges: Vec<&Edge> = v.in_edges.iter().map(|idx| &edges[idx]).collect();
            if in_edges
                .iter()
                .any(|e| matches!(e.state, EdgeState::Pending))
            {
                // not all inputs are ready
                debug!(
                    "defer connecting [{}] until all incoming edges are ready",
                    v.op_id
                );
                unconnected_vertices.push(v);
                continue;
            }

            connect_vertex(builder, registry, &mut edges, &inputs, v)?;
        }

        // can't connect anything and there are still remaining vertices
        if unconnected_vertices.len() > 0 && ws_length == unconnected_vertices.len() {
            warn!(
                "the following operations are not connected {:?}",
                unconnected_vertices
                    .iter()
                    .map(|v| v.op_id)
                    .collect::<Vec<_>>()
            );
            return Err(DiagramError::BadInterconnectChain);
        }
    }

    // connect terminate
    for edge_id in terminate_edges {
        let edge = edges.remove(&edge_id).ok_or(unknown_diagram_error!())?;
        match edge.state {
            EdgeState::Ready { output, origin: _ } => {
                let serialized_output = registry.data.serialize(builder, output)?;
                builder.connect(serialized_output, scope.terminate);
            }
            EdgeState::Pending => return Err(DiagramError::BadInterconnectChain),
        }
    }

    Ok(())
}

fn connect_vertex<'a>(
    builder: &mut Builder,
    registry: &NodeRegistry,
    edges: &mut HashMap<usize, Edge<'a>>,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    target: &'a Vertex,
) -> Result<(), DiagramError> {
    debug!("connecting [{}]", target.op_id);
    match target.op {
        // join needs all incoming edges to be connected at once so it is done at the vertex level
        // instead of per edge level.
        DiagramOperation::Join(join_op) => {
            if target.in_edges.is_empty() {
                return Err(DiagramError::EmptyJoin);
            }
            let mut outputs: HashMap<SourceOperation, DynOutput> = target
                .in_edges
                .iter()
                .map(|e| {
                    let edge = edges.remove(e).ok_or(unknown_diagram_error!())?;
                    match edge.state {
                        EdgeState::Ready { output, origin: _ } => Ok((edge.source, output)),
                        // "expected all incoming edges to be ready"
                        _ => Err(unknown_diagram_error!()),
                    }
                })
                .collect::<Result<HashMap<_, _>, _>>()?;

            let mut ordered_outputs: Vec<DynOutput> = Vec::with_capacity(target.in_edges.len());
            for source_id in join_op.inputs.iter() {
                let o = outputs
                    .remove(source_id)
                    .ok_or(DiagramError::OperationNotFound(source_id.to_string()))?;
                ordered_outputs.push(o);
            }

            let joined_output = if join_op.no_serialize.unwrap_or(false) {
                registry.data.join(
                    &ordered_outputs[0].type_id.clone(),
                    builder,
                    ordered_outputs,
                )?
            } else {
                serialize_and_join(builder, &registry.data, ordered_outputs)?.into()
            };

            let out_edge = edges
                .get_mut(&target.out_edges[0])
                .ok_or(unknown_diagram_error!())?;
            out_edge.state = EdgeState::Ready {
                output: joined_output,
                origin: None,
            };
            Ok(())
        }
        // for other operations, each edge is independent, so we can connect at the edge level.
        _ => {
            for edge_id in target.in_edges.iter() {
                connect_edge(builder, registry, edges, inputs, *edge_id, target)?;
            }
            Ok(())
        }
    }
}

fn connect_edge<'a>(
    builder: &mut Builder,
    registry: &NodeRegistry,
    edges: &mut HashMap<usize, Edge<'a>>,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    edge_id: usize,
    target: &Vertex,
) -> Result<(), DiagramError> {
    let edge = edges.remove(&edge_id).ok_or(unknown_diagram_error!())?;
    debug!(
        "connect edge {:?}, source: {:?}, target: {:?}",
        edge_id, edge.source, edge.target
    );
    let (output, origin) = match edge.state {
        EdgeState::Ready {
            output,
            origin: origin_node,
        } => {
            if let Some(origin_node) = origin_node {
                (output, Some(origin_node))
            } else {
                (output, None)
            }
        }
        EdgeState::Pending => panic!("can only connect ready edges"),
    };

    match target.op {
        DiagramOperation::Node(_) => {
            let input = inputs[target.op_id];
            let deserialized_output = registry.data.deserialize(&input.type_id, builder, output)?;
            dyn_connect(builder, deserialized_output, input)?;
        }
        DiagramOperation::ForkClone(fork_clone_op) => {
            let amount = fork_clone_op.next.len();
            let outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                <DefaultImpl as DynForkClone<serde_json::Value>>::dyn_fork_clone(
                    builder, output, amount,
                )
            } else {
                registry.data.fork_clone(builder, output, amount)
            }?;
            for (o, e) in outputs.into_iter().zip(target.out_edges.iter()) {
                let out_edge = edges.get_mut(e).ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready { output: o, origin };
            }
        }
        DiagramOperation::Unzip(unzip_op) => {
            let outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                Err(DiagramError::NotUnzippable)
            } else {
                registry.data.unzip(builder, output)
            }?;
            if outputs.len() < unzip_op.next.len() {
                return Err(DiagramError::NotUnzippable);
            }
            for (o, e) in outputs.into_iter().zip(target.out_edges.iter()) {
                let out_edge = edges.get_mut(e).ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready { output: o, origin };
            }
        }
        DiagramOperation::ForkResult(_) => {
            let (ok, err) = if output.type_id == TypeId::of::<serde_json::Value>() {
                Err(DiagramError::CannotForkResult)
            } else {
                registry.data.fork_result(builder, output)
            }?;
            {
                let out_edge = edges
                    .get_mut(&target.out_edges[0])
                    .ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready { output: ok, origin };
            }
            {
                let out_edge = edges
                    .get_mut(&target.out_edges[1])
                    .ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready {
                    output: err,
                    origin,
                };
            }
        }
        DiagramOperation::Split(split_op) => {
            let mut outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                let chain = output.into_output::<serde_json::Value>()?.chain(builder);
                split_chain(chain, split_op)
            } else {
                registry.data.split(builder, output, split_op)
            }?;

            // Because of how we build `out_edges`, if the split op uses the `remaining` slot,
            // then the last item will always be the remaining edge.
            let remaining_edge_id = if split_op.remaining.is_some() {
                Some(target.out_edges.last().ok_or(unknown_diagram_error!())?)
            } else {
                None
            };
            let other_edge_ids = if split_op.remaining.is_some() {
                &target.out_edges[..(target.out_edges.len() - 1)]
            } else {
                &target.out_edges[..]
            };

            for e in other_edge_ids {
                let out_edge = edges.get_mut(e).ok_or(unknown_diagram_error!())?;
                let output = outputs
                    .outputs
                    .remove(out_edge.target)
                    .ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready { output, origin };
            }
            if let Some(remaining_edge_id) = remaining_edge_id {
                let out_edge = edges
                    .get_mut(remaining_edge_id)
                    .ok_or(unknown_diagram_error!())?;
                out_edge.state = EdgeState::Ready {
                    output: outputs.remaining,
                    origin,
                };
            }
        }
        DiagramOperation::Join(_) => {
            // join is connected at the vertex level
        }
        DiagramOperation::Transform(transform_op) => {
            let transformed_output = transform_output(builder, registry, output, transform_op)?;
            let out_edge = edges
                .get_mut(&target.out_edges[0])
                .ok_or(unknown_diagram_error!())?;
            out_edge.state = EdgeState::Ready {
                output: transformed_output.into(),
                origin,
            }
        }
        DiagramOperation::Dispose => {}
    }
    Ok(())
}

/// Connect a [`DynOutput`] to a [`DynInputSlot`]. Use this only when both the output and input
/// are type erased. To connect an [`Output`] to a [`DynInputSlot`] or vice versa, prefer converting
/// the type erased output/input slot to the typed equivalent.
///
/// ```text
/// builder.connect(output.into_output::<i64>()?, dyn_input)?;
/// ```
fn dyn_connect(
    builder: &mut Builder,
    output: DynOutput,
    input: DynInputSlot,
) -> Result<(), DiagramError> {
    if output.type_id != input.type_id {
        return Err(DiagramError::TypeMismatch);
    }
    struct TypeErased {}
    let typed_output = Output::<TypeErased>::new(output.scope(), output.id());
    let typed_input = InputSlot::<TypeErased>::new(input.scope(), input.id());
    builder.connect(typed_output, typed_input);
    Ok(())
}
