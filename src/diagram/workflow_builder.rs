use std::collections::{hash_map::Entry, HashMap};

use tracing::{debug, warn};

use crate::{
    diagram::DiagramErrorContext, unknown_diagram_error, Buffer, Builder, InputSlot, Output,
    StreamPack,
};

use super::{
    BoxedMessage, BuiltinTarget, Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode,
    DiagramOperation, DiagramScope, DynInputSlot, DynOutput, NextOperation, OperationId,
    SourceOperation,
};

pub(super) struct Vertex<'a> {
    pub(super) op_id: &'a OperationId,
    pub(super) op: &'a DiagramOperation,
    pub(super) in_edges: Vec<usize>,
    pub(super) out_edges: Vec<usize>,
}

pub(super) struct Edge<'a> {
    pub(super) source: SourceOperation,
    pub(super) target: &'a NextOperation,
    pub(super) output: Option<DynOutput>,
}

pub(super) struct EdgeBuilder<'a, 'b> {
    vertices: &'b mut HashMap<&'a OperationId, Vertex<'a>>,
    edges: &'b mut HashMap<usize, Edge<'a>>,
    terminate_edges: &'b mut Vec<usize>,
    source: SourceOperation,
}

impl<'a, 'b> EdgeBuilder<'a, 'b> {
    fn add_edge(&mut self, edge: Edge<'a>) -> Result<(), DiagramErrorCode> {
        let new_edge_id = self.edges.len();
        match &edge.source {
            SourceOperation::Source(op_id) => {
                let source = self
                    .vertices
                    .get_mut(op_id)
                    .ok_or(DiagramErrorCode::OperationNotFound(op_id.clone()))?;
                source.out_edges.push(new_edge_id);
            }
            SourceOperation::Builtin { builtin: _ } => {}
        }
        match &edge.target {
            NextOperation::Target(op_id) => {
                let target = self
                    .vertices
                    .get_mut(op_id)
                    .ok_or(DiagramErrorCode::OperationNotFound(op_id.clone()))?;
                target.in_edges.push(new_edge_id);
            }
            NextOperation::Builtin { builtin } => match builtin {
                BuiltinTarget::Terminate => {
                    self.terminate_edges.push(new_edge_id);
                }
                BuiltinTarget::Dispose => {}
            },
        }
        match self.edges.entry(new_edge_id) {
            Entry::Vacant(entry) => entry.insert(edge),
            Entry::Occupied(mut entry) => {
                entry.insert(edge);
                entry.into_mut()
            }
        };
        Ok(())
    }

    pub(super) fn add_output_edge(
        &mut self,
        target: &'a NextOperation,
        output: Option<DynOutput>,
    ) -> Result<(), DiagramErrorCode> {
        self.add_edge(Edge {
            source: self.source.clone(),
            target,
            output,
        })?;
        Ok(())
    }
}

pub(super) fn create_workflow<'a, Streams: StreamPack>(
    scope: DiagramScope<Streams>,
    builder: &mut Builder,
    registry: &DiagramElementRegistry,
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
                    output: Some(scope.input.into()),
                },
            );
            vertices
                .get_mut(&op_id)
                .ok_or(DiagramError {
                    context: DiagramErrorContext {
                        op_id: Some(op_id.clone()),
                    },
                    code: DiagramErrorCode::OperationNotFound(op_id.clone()),
                })?
                .in_edges
                .push(0);
        }
    };

    let mut inputs: HashMap<&OperationId, DynInputSlot> = HashMap::with_capacity(diagram.ops.len());

    let mut terminate_edges: Vec<usize> = Vec::new();

    let buffers = create_buffers(builder, diagram);

    // create all the edges
    for (op_id, op) in &diagram.ops {
        let with_context = |code: DiagramErrorCode| DiagramError {
            context: DiagramErrorContext {
                op_id: Some(op_id.clone()),
            },
            code,
        };

        let edge_builder = EdgeBuilder {
            vertices: &mut vertices,
            edges: &mut edges,
            terminate_edges: &mut terminate_edges,
            source: SourceOperation::Source(op_id.clone()),
        };

        match op {
            DiagramOperation::Node(op) => {
                op.build_edges(builder, registry, &mut inputs, op_id, edge_builder)
                    .map_err(with_context)?;
            }
            DiagramOperation::ForkClone(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Unzip(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::ForkResult(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Split(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Join(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Transform(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Buffer(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::BufferAccess(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Listen(op) => {
                op.build_edges(edge_builder, builder, &buffers)
                    .map_err(with_context)?;
            }
        }
    }

    // iteratively connect edges
    let mut unconnected_vertices: Vec<&Vertex> = vertices.values().collect();
    while unconnected_vertices.len() > 0 {
        let ws = unconnected_vertices.clone();
        let ws_length = ws.len();
        unconnected_vertices.clear();

        for v in ws {
            let connected = connect_vertex(builder, registry, &mut edges, &inputs, &buffers, v)
                .map_err(|code| DiagramError {
                    context: DiagramErrorContext {
                        op_id: Some(v.op_id.clone()),
                    },
                    code,
                })?;
            if !connected {
                unconnected_vertices.push(v);
                continue;
            }
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
            return Err(DiagramError {
                context: DiagramErrorContext { op_id: None },
                code: DiagramErrorCode::IncompleteDiagram,
            });
        }
    }

    // connect terminate
    for edge_id in terminate_edges {
        let edge = edges.get_mut(&edge_id).ok_or(DiagramError {
            context: DiagramErrorContext { op_id: None },
            code: unknown_diagram_error!(),
        })?;
        match edge.output.take() {
            Some(output) => {
                debug!(
                    "connect edge {:?}, source: {:?}, target: {:?}",
                    edge_id, edge.source, edge.target
                );
                let serialized_output =
                    registry
                        .messages
                        .serialize(builder, output)
                        .map_err(|code| DiagramError {
                            context: DiagramErrorContext {
                                op_id: Some(
                                    NextOperation::Builtin {
                                        builtin: BuiltinTarget::Terminate,
                                    }
                                    .to_string(),
                                ),
                            },
                            code,
                        })?;
                builder.connect(serialized_output, scope.terminate);
            }
            None => {
                return Err(DiagramError {
                    context: DiagramErrorContext { op_id: None },
                    code: DiagramErrorCode::IncompleteDiagram,
                })
            }
        }
    }

    Ok(())
}

fn create_buffers<'a>(
    builder: &mut Builder,
    diagram: &'a Diagram,
) -> HashMap<&'a OperationId, Buffer<BoxedMessage>> {
    // because it is not known what would get sent to a buffer at compile time, the buffer
    // must be able to accept any type. `Buffer<serde_json::Value>` is an alternative, but
    // that limits support to only serializable type.
    //
    // TODO(koonpeng): Typed buffer is possible by registering a node. It is not very
    // intuitive as you would need to create a [`bevy_impulse::Node`] manually.
    // ```rust
    // let buffer = builder.create_buffer(...);
    // let node = Node { input: buffer.input_slot(), output: buffer.join(builder).output(), streams: () };
    // ```
    // A wrapper to do that can be considered.
    let buffers = diagram
        .ops
        .iter()
        .filter_map(|(op_id, op)| match op {
            DiagramOperation::Buffer(desc) => {
                // should we support unboxed buffers?
                Some((op_id, builder.create_buffer::<BoxedMessage>(desc.settings)))
            }
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    buffers
}

/// Given a [`Vertex`]
///   1. check that it has a single input edge and that the edge is ready
///   2. take the output from the input edge
///   3. borrows the output edges
/// Return a tuple of the taken output and the borrowed edges, if the input edge is not ready,
/// returns `None`.
pub fn map_one_to_many_edges<'a, 'b>(
    target: &Vertex,
    edges: &'b mut HashMap<usize, Edge<'a>>,
) -> Result<Option<(DynOutput, Vec<&'b mut Edge<'a>>)>, DiagramErrorCode> {
    // rust doesn't normally allow borrowing multiple values from the HashMap, so instead,
    // we create a new HashMap by borrowing all values, then `remove` from this HashMap for
    // values that we need to mutably borrow.
    let mut borrowed_edges: HashMap<usize, &mut Edge> =
        HashMap::from_iter(edges.iter_mut().map(|(k, v)| (*k, v)));

    let input = if let Some(output) = borrowed_edges
        .remove(
            target
                .in_edges
                .get(0)
                .ok_or_else(|| unknown_diagram_error!())?,
        )
        .ok_or(unknown_diagram_error!())?
        .output
        .take()
    {
        output
    } else {
        return Ok(None);
    };
    let out_edges: Vec<&mut Edge> = target
        .out_edges
        .iter()
        .map(|edge_id| {
            borrowed_edges
                .remove(edge_id)
                .ok_or(unknown_diagram_error!())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some((input, out_edges)))
}

/// Attempt to connect all output edges of a vertex, returns true if the connection is done.
fn connect_vertex<'a>(
    builder: &mut Builder,
    registry: &DiagramElementRegistry,
    edges: &mut HashMap<usize, Edge<'a>>,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    buffers: &HashMap<&OperationId, Buffer<BoxedMessage>>,
    target: &'a Vertex,
) -> Result<bool, DiagramErrorCode> {
    let in_edges: Vec<&Edge> = target.in_edges.iter().map(|idx| &edges[idx]).collect();
    if in_edges.iter().any(|e| matches!(e.output, None)) {
        // not all inputs are ready
        debug!(
            "defer connecting [{}] until all incoming edges are ready",
            target.op_id
        );
        return Ok(false);
    }

    macro_rules! connect_one_to_many {
        ($op:ident) => {{
            let (input, out_edges) = if let Some(result) = map_one_to_many_edges(target, edges)? {
                result
            } else {
                // in theory this should never happen as we checked that all edges are ready
                // at the start of the function.
                return Err(unknown_diagram_error!());
            };
            $op.try_connect(builder, input, out_edges, &registry.messages)?;
            Ok(true)
        }};
    }

    debug!("connecting [{}]", target.op_id);

    match target.op {
        DiagramOperation::Node(op) => {
            let borrowed_edges = HashMap::from_iter(edges.iter_mut());
            op.try_connect(builder, target, borrowed_edges, &inputs, &registry.messages)
        }
        DiagramOperation::ForkClone(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::Unzip(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::ForkResult(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::Split(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::Join(op) => {
            let borrowed_edges = HashMap::from_iter(edges.iter_mut());
            op.try_connect(builder, target, borrowed_edges, &registry.messages)?;
            Ok(true)
        }
        DiagramOperation::Transform(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::Buffer(op) => {
            let borrowed_edges = edges.iter_mut().collect();
            op.try_connect(
                builder,
                target,
                borrowed_edges,
                &registry.messages,
                &buffers,
            )
        }
        DiagramOperation::BufferAccess(op) => {
            let borrowed_edges = edges.iter_mut().collect();
            op.try_connect(
                builder,
                target,
                borrowed_edges,
                &registry.messages,
                &buffers,
            )
        }
        DiagramOperation::Listen(_) => Ok(true),
    }
}

/// Connect a [`DynOutput`] to a [`DynInputSlot`]. Use this only when both the output and input
/// are type erased. To connect an [`Output`] to a [`DynInputSlot`] or vice versa, prefer converting
/// the type erased output/input slot to the typed equivalent.
///
/// ```text
/// builder.connect(output.into_output::<i64>()?, dyn_input)?;
/// ```
pub(super) fn dyn_connect(
    builder: &mut Builder,
    output: DynOutput,
    input: DynInputSlot,
) -> Result<(), DiagramErrorCode> {
    if output.type_info != input.type_info {
        return Err(DiagramErrorCode::TypeMismatch {
            source_type: output.type_info,
            target_type: input.type_info,
        });
    }
    struct TypeErased {}
    let typed_output = Output::<TypeErased>::new(output.scope(), output.id());
    let typed_input = InputSlot::<TypeErased>::new(input.scope(), input.id());
    builder.connect(typed_output, typed_input);
    Ok(())
}
