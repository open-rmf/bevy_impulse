use std::{any::TypeId, collections::HashMap};

use tracing::debug;

use crate::{Builder, Output, StreamPack};

use super::{
    fork_clone::DynForkClone, impls::DefaultImpl, split_chain, transform::transform_output,
    Diagram, DiagramError, DiagramOperation, DynInputSlot, DynOutput, DynScope, NodeRegistry,
    OperationId, SplitOpParams,
};

struct Vertex<'a> {
    op_id: &'a OperationId,
    op: &'a DiagramOperation,
    in_edges: Vec<usize>,
    out_edges: Vec<usize>,
}

struct Edge<'a> {
    source: &'a OperationId,
    target: &'a OperationId,
    state: EdgeState<'a>,
}

enum EdgeState<'a> {
    Ready {
        output: DynOutput,
        origin: &'a OperationId,
    },
    Pending,
}

pub(super) fn create_workflow<'a, Streams: StreamPack>(
    scope: DynScope<Streams>,
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
    let mut edges: HashMap<usize, Edge> = HashMap::with_capacity(diagram.ops.len() * 2);

    // process start separately because we need to consume the scope input
    let (start_op_id, start_op) = vertices
        .iter()
        .find_map(|(op_id, v)| match v.op {
            DiagramOperation::Start(start_op) => Some((*op_id, start_op)),
            _ => None,
        })
        .ok_or(DiagramError::MissingStartOrTerminate)?;
    edges.insert(
        edges.len(),
        Edge {
            source: start_op_id,
            target: &start_op.next,
            state: EdgeState::Ready {
                output: scope.input.into(),
                origin: start_op_id,
            },
        },
    );
    vertices
        .get_mut(start_op_id)
        .ok_or_else(|| DiagramError::OperationNotFound(start_op_id.clone()))?
        .out_edges
        .push(0);
    vertices
        .get_mut(&start_op.next)
        .ok_or_else(|| DiagramError::OperationNotFound(start_op.next.clone()))?
        .in_edges
        .push(0);

    let mut inputs: HashMap<&OperationId, DynInputSlot> = HashMap::with_capacity(diagram.ops.len());

    // store the terminate input slot
    let terminate_op_id = vertices
        .iter()
        .find_map(|(op_id, v)| match v.op {
            DiagramOperation::Terminate(_) => Some(op_id),
            _ => None,
        })
        .ok_or(DiagramError::MissingStartOrTerminate)?;
    inputs.insert(terminate_op_id, scope.terminate.into());

    let mut add_edge = |source: &'a OperationId,
                        target: &'a OperationId,
                        state: EdgeState<'a>|
     -> Result<(), DiagramError> {
        edges.insert(
            edges.len(),
            Edge {
                source,
                target,
                state,
            },
        );
        let new_edge_idx = edges.len() - 1;

        let source_vertex = vertices
            .get_mut(source)
            .ok_or_else(|| DiagramError::OperationNotFound(source.clone()))?;
        source_vertex.out_edges.push(new_edge_idx);
        let target_vertex = vertices
            .get_mut(target)
            .ok_or_else(|| DiagramError::OperationNotFound(target.clone()))?;
        target_vertex.in_edges.push(new_edge_idx);
        Ok(())
    };

    // create all the edges
    for (op_id, op) in &diagram.ops {
        match op {
            DiagramOperation::Start(_) => {}
            DiagramOperation::Terminate(_) => {}
            DiagramOperation::Node(node_op) => {
                let reg = registry.get_registration(&node_op.node_id)?;
                let n = reg.create_node(builder, node_op.config.clone())?;
                inputs.insert(op_id, n.input);
                add_edge(
                    op_id,
                    &node_op.next,
                    EdgeState::Ready {
                        output: n.output.into(),
                        origin: op_id,
                    },
                )?;
            }
            DiagramOperation::ForkClone(fork_clone_op) => {
                for next_op_id in fork_clone_op.next.iter() {
                    add_edge(op_id, next_op_id, EdgeState::Pending)?;
                }
            }
            DiagramOperation::Unzip(unzip_op) => {
                for next_op_id in unzip_op.next.iter() {
                    add_edge(op_id, next_op_id, EdgeState::Pending)?;
                }
            }
            DiagramOperation::ForkResult(fork_result_op) => {
                add_edge(op_id, &fork_result_op.ok, EdgeState::Pending)?;
                add_edge(op_id, &fork_result_op.err, EdgeState::Pending)?;
            }
            DiagramOperation::Split(split_op) => {
                let next_op_ids: Vec<&OperationId> = match &split_op.params {
                    SplitOpParams::Index(v) => v.iter().collect(),
                    SplitOpParams::Key(v) => v.values().collect(),
                };
                for next_op_id in next_op_ids {
                    add_edge(op_id, next_op_id, EdgeState::Pending)?;
                }
                if let Some(remaining) = &split_op.remaining {
                    add_edge(op_id, &remaining, EdgeState::Pending)?;
                }
            }
            DiagramOperation::Join(join_op) => {
                add_edge(op_id, &join_op.next, EdgeState::Pending)?;
            }
            DiagramOperation::Transform(transform_op) => {
                add_edge(op_id, &transform_op.next, EdgeState::Pending)?;
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
                unconnected_vertices.push(v);
                continue;
            }

            for idx in &v.in_edges {
                connect_edge(builder, registry, &vertices, &mut edges, &inputs, *idx, v)?;
            }
        }

        // can't connect anything and there are still remaining vertices
        if unconnected_vertices.len() > 0 && ws_length == unconnected_vertices.len() {
            return Err(DiagramError::BadInterconnectChain);
        }
    }

    Ok(())
}

fn connect_edge<'a>(
    builder: &mut Builder,
    registry: &NodeRegistry,
    vertices: &'a HashMap<&OperationId, Vertex>,
    edges: &mut HashMap<usize, Edge<'a>>,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    edge_id: usize,
    target: &Vertex,
) -> Result<(), DiagramError> {
    let edge = edges.remove(&edge_id).unwrap();
    debug!(
        "connect edge, source: {}, target: {}",
        edge.source, edge.target
    );
    let (output, origin) = match edge.state {
        EdgeState::Ready { output, origin } => (output, &vertices[origin]),
        EdgeState::Pending => return Ok(()),
    };
    match target.op {
        DiagramOperation::Start(_) => return Err(DiagramError::CannotConnectStart),
        DiagramOperation::Terminate(_) => {
            let serialized_output = serialize(builder, registry, output, origin)?;
            let input = inputs[target.op_id];
            builder.connect(serialized_output, input.into_input());
        }
        DiagramOperation::Node(_) => {
            let input = inputs[target.op_id];
            let deserialized_output =
                deserialize(builder, registry, output, target, input.type_id)?;
            dyn_connect(builder, deserialized_output, input)?;
        }
        DiagramOperation::ForkClone(fork_clone_op) => {
            let amount = fork_clone_op.next.len();
            let outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                <DefaultImpl as DynForkClone<serde_json::Value>>::dyn_fork_clone(
                    builder, output, amount,
                )
            } else {
                match origin.op {
                    DiagramOperation::Node(node_op) => {
                        let reg = registry.get_registration(&node_op.node_id)?;
                        reg.fork_clone(builder, output, amount)
                    }
                    _ => Err(DiagramError::NotCloneable),
                }
            }?;
            outputs
                .into_iter()
                .zip(target.out_edges.iter())
                .for_each(|(o, e)| {
                    edges.insert(
                        *e,
                        Edge {
                            source: &edge.source,
                            target: &edge.target,
                            state: EdgeState::Ready {
                                output: o,
                                origin: origin.op_id,
                            },
                        },
                    );
                });
        }
        DiagramOperation::Unzip(unzip_op) => {
            let outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                Err(DiagramError::NotUnzippable)
            } else {
                match origin.op {
                    DiagramOperation::Node(node_op) => {
                        let reg = registry.get_registration(&node_op.node_id)?;
                        reg.unzip(builder, output)
                    }
                    _ => Err(DiagramError::NotUnzippable),
                }
            }?;
            if outputs.len() < unzip_op.next.len() {
                return Err(DiagramError::NotUnzippable);
            }
            outputs
                .into_iter()
                .zip(target.out_edges.iter())
                .for_each(|(o, e)| {
                    edges.insert(
                        *e,
                        Edge {
                            source: &edge.source,
                            target: &edge.target,
                            state: EdgeState::Ready {
                                output: o,
                                origin: origin.op_id,
                            },
                        },
                    );
                });
        }
        DiagramOperation::ForkResult(_) => {
            let (ok, err) = if output.type_id == TypeId::of::<serde_json::Value>() {
                Err(DiagramError::CannotForkResult)
            } else {
                match origin.op {
                    DiagramOperation::Node(node_op) => {
                        let reg = registry.get_registration(&node_op.node_id)?;
                        reg.fork_result(builder, output)
                    }
                    _ => Err(DiagramError::CannotForkResult),
                }
            }?;
            edges.insert(
                target.out_edges[0],
                Edge {
                    source: &edge.source,
                    target: &edge.target,
                    state: EdgeState::Ready {
                        output: ok,
                        origin: origin.op_id,
                    },
                },
            );
            edges.insert(
                target.out_edges[1],
                Edge {
                    source: &edge.source,
                    target: &edge.target,
                    state: EdgeState::Ready {
                        output: err,
                        origin: origin.op_id,
                    },
                },
            );
        }
        DiagramOperation::Split(split_op) => {
            let outputs = if output.type_id == TypeId::of::<serde_json::Value>() {
                let chain = output.into_output::<serde_json::Value>().chain(builder);
                split_chain(chain, split_op)
            } else {
                match origin.op {
                    DiagramOperation::Node(node_op) => {
                        let reg = registry.get_registration(&node_op.node_id)?;
                        reg.split(builder, output, split_op)
                    }
                    _ => Err(DiagramError::NotSplittable),
                }
            }?;
            outputs
                .outputs
                .into_iter()
                .zip(target.out_edges.iter())
                .for_each(|((_, o), e)| {
                    edges.insert(
                        *e,
                        Edge {
                            source: &edge.source,
                            target: &edge.target,
                            state: EdgeState::Ready {
                                output: o,
                                origin: origin.op_id,
                            },
                        },
                    );
                });
            if let Some(_) = &split_op.remaining {
                edges.insert(
                    *target.out_edges.last().unwrap(),
                    Edge {
                        source: &edge.source,
                        target: &edge.target,
                        state: EdgeState::Ready {
                            output: outputs.remaining,
                            origin: origin.op_id,
                        },
                    },
                );
            }
        }
        DiagramOperation::Join(_) => {
            panic!("not implemented")
        }
        DiagramOperation::Transform(transform_op) => {
            let transformed_output = transform_output(builder, registry, output, transform_op)?;
            edges.insert(
                target.out_edges[0],
                Edge {
                    source: &edge.source,
                    target: &edge.target,
                    state: EdgeState::Ready {
                        output: transformed_output.into(),
                        origin: origin.op_id,
                    },
                },
            );
        }
        DiagramOperation::Dispose => {}
    }
    Ok(())
}

fn dyn_connect(
    builder: &mut Builder,
    output: DynOutput,
    input: DynInputSlot,
) -> Result<(), DiagramError> {
    if output.type_id != input.type_id {
        Err(DiagramError::TypeMismatch)
    } else {
        struct TypeErased {}
        builder.connect(
            output.into_output::<TypeErased>(),
            input.into_input::<TypeErased>(),
        );
        Ok(())
    }
}

/// Try to deserialize `output` into `input_type`. If `output` is not `serde_json::Value`, this does nothing.
fn deserialize(
    builder: &mut Builder,
    registry: &NodeRegistry,
    output: DynOutput,
    target: &Vertex,
    input_type: TypeId,
) -> Result<DynOutput, DiagramError> {
    if output.type_id != TypeId::of::<serde_json::Value>() || output.type_id == input_type {
        Ok(output)
    } else {
        let serialized = output.into_output::<serde_json::Value>();
        match target.op {
            DiagramOperation::Node(node_op) => {
                let reg = registry.get_registration(&node_op.node_id)?;
                if reg.metadata.request.deserializable {
                    let deserialize_impl = &registry.deserialize_impls[&input_type];
                    Ok(deserialize_impl(builder, serialized))
                } else {
                    Err(DiagramError::NotSerializable)
                }
            }
            _ => Err(DiagramError::NotSerializable),
        }
    }
}

fn serialize(
    builder: &mut Builder,
    registry: &NodeRegistry,
    output: DynOutput,
    origin: &Vertex,
) -> Result<Output<serde_json::Value>, DiagramError> {
    if output.type_id == TypeId::of::<serde_json::Value>() {
        Ok(output.into_output())
    } else {
        match origin.op {
            DiagramOperation::Start(_) => Ok(output.into_output()),
            DiagramOperation::Node(node_op) => {
                let reg = registry.get_registration(&node_op.node_id)?;
                if reg.metadata.response.serializable {
                    let serialize_impl = &registry.serialize_impls[&output.type_id];
                    Ok(serialize_impl(builder, output))
                } else {
                    Err(DiagramError::NotSerializable)
                }
            }
            _ => Err(DiagramError::NotSerializable),
        }
    }
}
