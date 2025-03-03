use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::{debug, warn};

use crate::{diagram::DiagramErrorContext, AnyBuffer, Builder, InputSlot, Output, StreamPack};

use super::{
    BuiltinTarget, Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode,
    DiagramOperation, DiagramScope, DynInputSlot, DynOutput, NextOperation, OperationId,
    SourceOperation,
};

pub(super) struct Vertex {
    pub(super) op_id: OperationId,
    pub(super) op: DiagramOperation,
    pub(super) in_edges: Vec<Arc<Mutex<Edge>>>,
    pub(super) out_edges: Vec<Arc<Mutex<Edge>>>,
}

pub(super) struct Edge {
    pub(super) source: SourceOperation,
    pub(super) target: NextOperation,
    pub(super) output: Option<DynOutput>,
    pub(super) tag: Option<String>,
}

pub(super) struct EdgeBuilder<'a, 'b> {
    vertices: &'b mut HashMap<&'a OperationId, Vertex>,
    terminate_edges: &'b mut Vec<Arc<Mutex<Edge>>>,
    source: SourceOperation,
}

impl<'a, 'b> EdgeBuilder<'a, 'b> {
    fn add_edge(&mut self, edge: Edge) -> Result<(), DiagramErrorCode> {
        let source = edge.source.clone();
        let target = edge.target.clone();
        let new_edge = Arc::new(Mutex::new(edge));
        match source {
            SourceOperation::Source(op_id) => {
                let source = self
                    .vertices
                    .get_mut(&op_id)
                    .ok_or(DiagramErrorCode::OperationNotFound(op_id.clone()))?;
                source.out_edges.push(Arc::clone(&new_edge));
            }
            SourceOperation::Builtin { builtin: _ } => {}
        }
        match target {
            NextOperation::Target(op_id) => {
                let target = self
                    .vertices
                    .get_mut(&op_id)
                    .ok_or(DiagramErrorCode::OperationNotFound(op_id))?;
                target.in_edges.push(Arc::clone(&new_edge));
            }
            NextOperation::Section { section, input: _ } => {
                let target = self
                    .vertices
                    .get_mut(&section)
                    .ok_or(DiagramErrorCode::OperationNotFound(section))?;
                target.in_edges.push(Arc::clone(&new_edge));
            }
            NextOperation::Builtin { builtin } => match builtin {
                BuiltinTarget::Terminate => {
                    self.terminate_edges.push(new_edge);
                }
                BuiltinTarget::Dispose => {}
            },
        }
        Ok(())
    }

    pub(super) fn add_output_edge(
        &mut self,
        target: NextOperation,
        output: Option<DynOutput>,
        tag: Option<String>,
    ) -> Result<(), DiagramErrorCode> {
        self.add_edge(Edge {
            source: self.source.clone(),
            target,
            output,
            tag,
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
                    op_id: op_id.clone(),
                    op: op.clone(),
                    in_edges: Vec::new(),
                    out_edges: Vec::new(),
                },
            )
        })
        .collect();

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
            let out_edge = Edge {
                source: SourceOperation::Builtin {
                    builtin: super::BuiltinSource::Start,
                },
                target: diagram.start.clone(),
                output: Some(scope.input.into()),
                tag: None,
            };
            vertices
                .get_mut(&op_id)
                .ok_or(DiagramError {
                    context: DiagramErrorContext {
                        op_id: Some(op_id.clone()),
                    },
                    code: DiagramErrorCode::OperationNotFound(op_id.clone()),
                })?
                .in_edges
                .push(Arc::new(Mutex::new(out_edge)));
        }
        NextOperation::Section { section, input } => {
            let out_edge = Edge {
                source: SourceOperation::Builtin {
                    builtin: super::BuiltinSource::Start,
                },
                target: diagram.start.clone(),
                output: Some(scope.input.into()),
                tag: Some(input.clone()),
            };
            vertices
                .get_mut(&section)
                .ok_or(DiagramError {
                    context: DiagramErrorContext {
                        op_id: Some(section.clone()),
                    },
                    code: DiagramErrorCode::OperationNotFound(section.clone()),
                })?
                .in_edges
                .push(Arc::new(Mutex::new(out_edge)));
        }
    };

    let mut inputs: HashMap<&OperationId, DynInputSlot> = HashMap::with_capacity(diagram.ops.len());

    let mut terminate_edges: Vec<Arc<Mutex<Edge>>> = Vec::new();

    let mut buffers: HashMap<OperationId, AnyBuffer> = HashMap::new();

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
            DiagramOperation::SerializedJoin(op) => {
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
                op.build_edges(edge_builder).map_err(with_context)?;
            }
            DiagramOperation::Section(op) => {
                op.build_edges(edge_builder).map_err(with_context)?;
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
            let connected = connect_vertex(builder, v, registry, &inputs, &mut buffers, diagram)
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
                    .map(|v| v.op_id.clone())
                    .collect::<Vec<_>>()
            );
            return Err(DiagramError {
                context: DiagramErrorContext { op_id: None },
                code: DiagramErrorCode::IncompleteDiagram,
            });
        }
    }

    // connect terminate
    for edge in terminate_edges {
        let mut edge = edge.try_lock().unwrap();
        match edge.output.take() {
            Some(output) => {
                debug!(
                    "connect edge, source: {:?}, target: {:?}",
                    edge.source, edge.target
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

/// Attempt to connect all output edges of a vertex, returns true if the connection is done.
fn connect_vertex<'a>(
    builder: &mut Builder,
    target: &Vertex,
    registry: &DiagramElementRegistry,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    buffers: &mut HashMap<OperationId, AnyBuffer>,
    diagram: &Diagram,
) -> Result<bool, DiagramErrorCode> {
    if target
        .in_edges
        .iter()
        .any(|e| matches!(e.try_lock().unwrap().output, None))
    {
        // not all inputs are ready
        debug!(
            "defer connecting [{}] until all incoming edges are ready",
            target.op_id
        );
        return Ok(false);
    }

    debug!("connecting [{}]", target.op_id);

    match &target.op {
        DiagramOperation::Node(op) => op.try_connect(target, builder, &inputs, &registry.messages),
        DiagramOperation::ForkClone(op) => op.try_connect(target, builder, &registry.messages),
        DiagramOperation::Unzip(op) => op.try_connect(target, builder, &registry.messages),
        DiagramOperation::ForkResult(op) => op.try_connect(target, builder, &registry.messages),
        DiagramOperation::Split(op) => op.try_connect(target, builder, &registry.messages),
        DiagramOperation::Join(op) => op.try_connect(target, builder, &registry, buffers, diagram),
        DiagramOperation::SerializedJoin(op) => op.try_connect(target, builder, &buffers),
        DiagramOperation::Transform(op) => op.try_connect(target, builder, &registry.messages),
        DiagramOperation::Buffer(op) => {
            op.try_connect(target, builder, &registry.messages, buffers)
        }
        DiagramOperation::BufferAccess(op) => {
            op.try_connect(target, builder, registry, buffers, diagram)
        }
        DiagramOperation::Listen(op) => {
            op.try_connect(target, builder, registry, &buffers, diagram)
        }
        DiagramOperation::Section(op) => op.try_connect(target, builder, registry, buffers),
    }
}

/// Connect a [`DynOutput`] to a [`DynInputSlot`]. Use this only when both the output and input
/// are type erased. To connect an [`Output`] to a [`DynInputSlot`] or vice versa, prefer converting
/// the type erased output/input slot to the typed equivalent.
///
/// ```text
/// builder.connect(output.into_output::<i64>()?, dyn_input)?;
/// ```
pub fn dyn_connect(
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

pub(super) fn validate_single_input(vertex: &Vertex) -> Result<DynOutput, DiagramErrorCode> {
    if vertex.in_edges.len() != 1 {
        return Err(DiagramErrorCode::OnlySingleInput);
    } else {
        Ok(vertex.in_edges[0]
            .try_lock()
            .unwrap()
            .output
            .take()
            .unwrap())
    }
}
