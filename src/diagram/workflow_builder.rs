/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use tracing::{debug, warn};

use crate::{
    diagram::DiagramErrorContext, unknown_diagram_error, AnyBuffer, Builder, InputSlot, Output,
    StreamPack, BufferMap, BufferIdentifier, JsonMessage,
};

use super::{
    BuiltinTarget, Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode,
    DiagramOperation, DiagramScope, DynInputSlot, DynOutput, NextOperation, OperationId,
    SourceOperation, BufferInputs, TypeInfo,
};

#[derive(Debug)]
pub(super) struct Vertex<'a> {
    pub(super) op_id: &'a OperationId,
    pub(super) op: &'a DiagramOperation,
    pub(super) in_edges: Vec<usize>,
    pub(super) out_edges: Vec<usize>,
}


struct EdgeKey {
    source: SourceOperation,
    target: NextOperation,
}

#[derive(Debug)]
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

pub struct ConnectionContext<'a> {
    pub diagram: &'a Diagram,
    pub registry: &'a DiagramElementRegistry,
}

pub type ConnectFn = Box<dyn FnMut(&OperationId, DynOutput, &mut Builder, ConnectionContext) -> Result<(), DiagramErrorCode>>;

pub struct DiagramConstruction {
    input_for_operation: HashMap<OperationId, ConnectFn>,
    outputs_to_operation_target: HashMap<OperationId, Vec<DynOutput>>,
    outputs_to_builtin_target: HashMap<BuiltinTarget, Vec<DynOutput>>,
    buffers: HashMap<OperationId, AnyBuffer>,
}

impl DiagramConstruction {
    /// Get all the currently known outputs that are aimed at this target operation.
    pub fn get_outputs_into_operation_target(&self, id: &OperationId) -> Option<&Vec<DynOutput>> {
        self.outputs_to_operation_target.get(id)
    }

    /// Get a single currently known output that is aimed at this target operation.
    /// This can be used to infer information for building the operation.
    ///
    /// The workflow builder will ensure that all outputs targeting this
    /// operation share the same message type, so getting a single sample is
    /// usually enough to infer what is needed to build the operation.
    pub fn get_sample_output_into_target(&self, id: &OperationId) -> Option<&DynOutput> {
        self.get_outputs_into_operation_target(id).and_then(|outputs| outputs.first())
    }

    pub fn add_output_into_target(&mut self, target: NextOperation, output: DynOutput) {
        match target {
            NextOperation::Target(id) => {
                self.outputs_to_operation_target.entry(id).or_default().push(output);
            }
            NextOperation::Builtin { builtin } => {
                self.outputs_to_builtin_target.entry(builtin).or_default().push(output);
            }
        }
    }

    /// Set the input slot of an operation. This should not be called more than
    /// once per operation, because only one input slot can be used for any
    /// operation.
    pub fn set_input_for_target(
        &mut self,
        operation: &OperationId,
        input: DynInputSlot,
    ) -> Result<(), DiagramErrorCode> {
        self.set_connect_into_target(
            operation,
            move |_, output, builder, _| output.connect_to(&input, builder),
        )
    }

    /// Set a callback that will allow outputs to connect into this target. This
    /// is a more general method than [`Self::set_input_for_target`]. This allows
    /// you to take further action before a target is connected, like serializing.
    pub fn set_connect_into_target(
        &mut self,
        operation: &OperationId,
        connect: impl FnMut(&OperationId, DynOutput, &mut Builder, ConnectionContext) -> Result<(), DiagramErrorCode> + 'static,
    ) -> Result<(), DiagramErrorCode> {
        let f = Box::new(connect);
        match self.input_for_operation.entry(operation.clone()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleInputsCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(f);
            }
        }
        Ok(())
    }

    /// Set the buffer that should be used for a certain operation. This will
    /// also set its connection callback.
    pub fn set_buffer_for_operation(
        &mut self,
        operation: &OperationId,
        buffer: AnyBuffer,
    ) -> Result<(), DiagramErrorCode> {
        match self.buffers.entry(operation.clone()) {
            Entry::Occupied(_) => {
                return Err(DiagramErrorCode::MultipleBuffersCreated(operation.clone()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(buffer);
            }
        }

        let input: DynInputSlot = buffer.into();
        self.set_input_for_target(operation, input)
    }

    /// Create a buffer map based on the buffer inputs provided. If one or more
    /// of the buffers in BufferInputs is not available, get an error including
    /// the name of the missing buffer.
    pub fn create_buffer_map(&self, inputs: &BufferInputs) -> Result<BufferMap, String> {
        let attempt_get_buffer = |name: &String| -> Result<AnyBuffer, String> {
            self.buffers.get(name).copied().ok_or_else(|| {
                format!("cannot find buffer named [{name}]")
            })
        };

        match inputs {
            BufferInputs::Single(op_id) => {
                let mut buffer_map = BufferMap::with_capacity(1);
                buffer_map.insert(BufferIdentifier::Index(0), attempt_get_buffer(op_id)?);
                Ok(buffer_map)
            }
            BufferInputs::Dict(mapping) => {
                let mut buffer_map = BufferMap::with_capacity(mapping.len());
                for (k, op_id) in mapping {
                    buffer_map.insert(
                        BufferIdentifier::Name(k.clone().into()),
                        attempt_get_buffer(op_id)?,
                    );
                }
                Ok(buffer_map)
            }
            BufferInputs::Array(arr) => {
                let mut buffer_map = BufferMap::with_capacity(arr.len());
                for (i, op_id) in arr.into_iter().enumerate() {
                    buffer_map.insert(
                        BufferIdentifier::Index(i),
                        attempt_get_buffer(op_id)?,
                    );
                }
                Ok(buffer_map)
            }
        }
    }
}

pub struct DiagramContext<'a> {
    pub construction: &'a mut DiagramConstruction,
    pub diagram: &'a Diagram,
    pub registry: &'a DiagramElementRegistry,
}

impl<'a> DiagramContext<'a> {
    /// Get the type information for the request message that goes into a node.
    ///
    /// # Arguments
    ///
    /// * `target` - Optionally indicate a specific node in the diagram to treat
    ///   as the target node, even if it is not the actual target. Using [`Some`]
    ///   for this will override whatever is used for `next`.
    /// * `next` - Indicate the next operation, i.e. the true target.
    pub fn get_node_request_type(
        &self,
        target: Option<&OperationId>,
        next: &NextOperation
    ) -> Result<TypeInfo, DiagramErrorCode> {
        let target_node = if let Some(target) = target {
            self.diagram.get_op(target)?
        } else {
            match next {
                NextOperation::Target(op_id) => self.diagram.get_op(op_id)?,
                NextOperation::Builtin { builtin } => match builtin {
                    BuiltinTarget::Terminate => return Ok(TypeInfo::of::<JsonMessage>()),
                    BuiltinTarget::Dispose => return Err(DiagramErrorCode::UnknownTarget),
                },
            }
        };
        let node_op = match target_node {
            DiagramOperation::Node(op) => op,
            _ => return Err(DiagramErrorCode::UnknownTarget),
        };
        let target_type = self.registry.get_node_registration(&node_op.builder)?.request;
        Ok(target_type)
    }
}

/// Indicate whether the operation has finished building.
#[derive(Debug, Clone)]
pub enum BuildStatus {
    /// The operation has finished building.
    Finished,
    /// The operation needs to make another attempt at building after more
    /// information becomes available.
    Defer {
        /// Progress was made during this run. This can mean the operation has
        /// added some information into the diagram but might need to provide
        /// more later. If no operations make any progress within an entire
        /// iteration of the workflow build then we assume it is impossible to
        /// build the diagram.
        progress: bool,
        reason: Cow<'static, str>,
    }
}

impl BuildStatus {
    /// Indicate that the build of the operation needs to be deferred.
    pub fn defer(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Defer {
            progress: false,
            reason: reason.into(),
        }
    }

    /// Indicate that the operation made progress, even if it's deferred.
    pub fn with_progress(mut self) -> Self {
        match &mut self {
            Self::Defer { progress, .. }  => {
                *progress = true;
            }
            Self::Finished => {
                // Do nothing
            }
        }

        self
    }

    /// Did this operation make progress on this round?
    pub fn made_progress(&self) -> bool {
        match self {
            Self::Defer { progress, .. } => *progress,
            Self::Finished => true,
        }
    }
}

pub trait BuildDiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode>;
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

    let mut input_for_operation: HashMap<&OperationId, DynInputSlot> = HashMap::new();
    let mut outputs_to_target: HashMap<&NextOperation, Vec<DynOutput>> = HashMap::new();

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

    let mut buffers: HashMap<&OperationId, AnyBuffer> = HashMap::new();

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
        }
    }

    // iteratively connect edges
    let mut unconnected_vertices: Vec<&Vertex> = vertices.values().collect();
    while unconnected_vertices.len() > 0 {
        let ws = unconnected_vertices.clone();
        let ws_length = ws.len();
        unconnected_vertices.clear();

        for v in ws {
            let connected = connect_vertex(
                builder,
                v,
                registry,
                &mut edges,
                &inputs,
                &mut buffers,
                diagram,
            )
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
                .ok_or(DiagramErrorCode::OnlySingleInput)?,
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
    target: &'a Vertex,
    registry: &DiagramElementRegistry,
    edges: &mut HashMap<usize, Edge<'a>>,
    inputs: &HashMap<&OperationId, DynInputSlot>,
    buffers: &mut HashMap<&'a OperationId, AnyBuffer>,
    diagram: &Diagram,
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
            op.try_connect(
                builder,
                target,
                borrowed_edges,
                &registry,
                &buffers,
                diagram,
            )
        }
        DiagramOperation::SerializedJoin(op) => {
            let borrowed_edges = HashMap::from_iter(edges.iter_mut());
            op.try_connect(builder, target, borrowed_edges, &buffers)
        }
        DiagramOperation::Transform(op) => {
            connect_one_to_many!(op)
        }
        DiagramOperation::Buffer(op) => {
            let borrowed_edges = edges.iter_mut().collect();
            op.try_connect(builder, target, borrowed_edges, buffers, &registry.messages)
        }
        DiagramOperation::BufferAccess(op) => {
            let borrowed_edges = edges.iter_mut().collect();
            op.try_connect(builder, target, borrowed_edges, registry, buffers, diagram)
        }
        DiagramOperation::Listen(op) => {
            let borrowed_edges = edges.iter_mut().collect();
            op.try_connect(builder, target, borrowed_edges, registry, buffers, diagram)
        }
    }
}
