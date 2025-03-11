use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use crate::{
    diagram::{type_info::TypeInfo, DiagramErrorContext},
    AnyBuffer, Builder, InputSlot, Output,
};

use super::{
    Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode, DiagramOperation,
    DynInputSlot, DynOutput, IntoOperationId, MessageRegistry, NextOperation, OperationId,
};

pub(super) struct Edge {
    target: OperationId,
    output: Arc<Mutex<Option<DynOutput>>>,
}

impl Edge {
    pub(super) fn target(&self) -> &OperationId {
        &self.target
    }

    /// Take the output of an edge, this will panic if the edge does not have an output.
    ///
    /// Calling this for an input edge from the connect handler of a vertex should never panic
    /// because the workflow builder ensures that all input edges are ready before calling the handler.
    pub(super) fn take_output(&self) -> DynOutput {
        self.output.try_lock().unwrap().take().unwrap()
    }

    pub(super) fn set_output(&self, output: DynOutput) {
        *self.output.try_lock().unwrap() = Some(output);
    }

    pub(super) fn has_output(&self) -> bool {
        self.output.try_lock().unwrap().is_some()
    }
}

pub(super) type ConnectVisitor<'a> = dyn Fn(
        &Vertex,
        &mut Builder,
        &DiagramElementRegistry,
        &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode>
    + 'a;

pub(super) struct Vertex {
    pub(super) in_edges: Vec<Edge>,
    pub(super) out_edges: Vec<Edge>,
}

struct VertexOperations<'a> {
    connect_visitor: Box<ConnectVisitor<'a>>,
}

pub(super) struct EdgeBuilder<'a> {
    vertex: &'a mut Vertex,
    target_aliases: &'a HashMap<OperationId, OperationId>,
}

impl<'a> EdgeBuilder<'a> {
    pub(super) fn add_output_edge(&mut self, target: NextOperation, output: Option<DynOutput>) {
        let target_op_id = target.into_operation_id();
        let target = if let Some(op_id) = self.target_aliases.get(&target_op_id) {
            op_id
        } else {
            &target_op_id
        };

        self.vertex.out_edges.push(Edge {
            target: target.clone(),
            output: Arc::new(Mutex::new(output)),
        });
    }
}

pub(super) struct WorkflowBuilder<'a> {
    vertices: HashMap<OperationId, (RefCell<Vertex>, VertexOperations<'a>)>,
    target_aliases: HashMap<OperationId, OperationId>,
}

impl<'a> WorkflowBuilder<'a> {
    pub(super) fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            target_aliases: HashMap::new(),
        }
    }

    pub(super) fn add_vertex<AsId, F>(&mut self, op_id: AsId, connect_visitor: F) -> EdgeBuilder
    where
        AsId: IntoOperationId,
        F: Fn(
                &Vertex,
                &mut Builder,
                &DiagramElementRegistry,
                &mut HashMap<OperationId, AnyBuffer>,
            ) -> Result<bool, DiagramErrorCode>
            + 'a,
    {
        let op_id = op_id.into_operation_id();
        let value = (
            RefCell::new(Vertex {
                in_edges: Vec::new(),
                out_edges: Vec::new(),
            }),
            VertexOperations {
                connect_visitor: Box::new(connect_visitor),
            },
        );
        let vertex = match self.vertices.entry(op_id.clone()) {
            Entry::Occupied(mut entry) => {
                entry.insert(value);
                entry.into_mut()
            }
            Entry::Vacant(entry) => entry.insert(value),
        }
        .0
        .get_mut();

        EdgeBuilder {
            vertex,
            target_aliases: &self.target_aliases,
        }
    }

    pub(super) fn add_vertices_from_op(
        &mut self,
        op_id: OperationId,
        op: &'a DiagramOperation,
        builder: &mut Builder,
        diagram: &'a Diagram,
        registry: &'a DiagramElementRegistry,
    ) -> Result<(), DiagramErrorCode> {
        match op {
            DiagramOperation::Node(op) => op.add_vertices(builder, self, op_id, registry),
            DiagramOperation::Section(op) => {
                op.add_vertices(op_id, self, builder, diagram, registry)
            }
            DiagramOperation::ForkClone(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::Unzip(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::ForkResult(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::Split(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::Join(op) => {
                op.add_vertices(self, op_id, diagram);
                Ok(())
            }
            DiagramOperation::SerializedJoin(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::Transform(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::Buffer(op) => {
                op.add_vertices(self, op_id);
                Ok(())
            }
            DiagramOperation::BufferAccess(op) => {
                op.add_vertices(self, op_id, diagram);
                Ok(())
            }
            DiagramOperation::Listen(op) => {
                op.add_vertices(self, op_id, diagram);
                Ok(())
            }
        }
    }

    /// Set aliases to redirect the edge target when adding edges.
    pub(super) fn set_target_aliases(&mut self, aliases: HashMap<OperationId, OperationId>) {
        self.target_aliases = aliases;
    }

    fn connect_edges(&mut self) -> Result<(), DiagramError> {
        for (op_id, (vertex, _)) in &self.vertices {
            for edge in &vertex.borrow().out_edges {
                let target = &self
                    .vertices
                    .get(&edge.target)
                    .ok_or_else(|| DiagramError {
                        code: DiagramErrorCode::OperationNotFound(edge.target.clone()),
                        context: DiagramErrorContext {
                            op_id: Some(op_id.clone()),
                        },
                    })?
                    .0;
                target.borrow_mut().in_edges.push(Edge {
                    target: edge.target.clone(),
                    output: Arc::clone(&edge.output),
                })
            }
        }
        Ok(())
    }

    pub(super) fn create_workflow(
        mut self,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
    ) -> Result<(), DiagramError> {
        self.connect_edges()?;
        let mut buffers = HashMap::new();

        // iteratively connect the vertices
        while !self.vertices.is_empty() {
            let before_len = self.vertices.len();
            let mut unconnected_vertices = HashMap::new();

            for (op_id, (mut v, mut ops)) in self.vertices.drain() {
                let vertex = v.get_mut();

                // skip if one or more edges is not ready yet
                if vertex.in_edges.iter().any(|edge| !edge.has_output()) {
                    unconnected_vertices.insert(op_id, (v, ops));
                    continue;
                }

                // need to take ownership to avoid multiple mutable borrows, it is always
                // put back so the unwrap should never fail.
                let connect_visitor = &mut ops.connect_visitor;
                let connected =
                    connect_visitor(vertex, builder, registry, &mut buffers).map_err(|code| {
                        DiagramError {
                            code,
                            context: DiagramErrorContext {
                                op_id: Some(op_id.clone()),
                            },
                        }
                    })?;

                if !connected {
                    unconnected_vertices.insert(op_id, (v, ops));
                }
            }
            self.vertices.extend(unconnected_vertices);

            // no progress is made and diagram is not fully connected
            if !self.vertices.is_empty() && self.vertices.len() == before_len {
                return Err(DiagramError {
                    code: DiagramErrorCode::IncompleteDiagram,
                    context: DiagramErrorContext { op_id: None },
                });
            }
        }

        Ok(())
    }
}

/// Connect a [`DynOutput`] to a [`DynInputSlot`]. If the output is a [`serde_json::Value`], then
/// this will attempt to deserialize it into the input type.
///
/// ```text
/// builder.connect(output.into_output::<i64>()?, dyn_input)?;
/// ```
pub fn dyn_connect(
    builder: &mut Builder,
    output: DynOutput,
    input: DynInputSlot,
    registry: &MessageRegistry,
) -> Result<(), DiagramErrorCode> {
    let output = if output.type_info != input.type_info {
        if output.type_info == TypeInfo::of::<serde_json::Value>() {
            registry.deserialize(&input.type_info, builder, output)?
        } else {
            return Err(DiagramErrorCode::TypeMismatch {
                source_type: output.type_info,
                target_type: input.type_info,
            });
        }
    } else {
        output
    };

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
        Ok(vertex.in_edges[0].take_output())
    }
}
