use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    diagram::{type_info::TypeInfo, DiagramErrorContext},
    AnyBuffer, Builder, InputSlot, Output,
};

use super::{
    DiagramElementRegistry, DiagramError, DiagramErrorCode, DynInputSlot, DynOutput,
    IntoOperationId, MessageRegistry, NextOperation, OperationId, SourceOperation,
};

pub(super) struct Edge {
    pub(super) source: SourceOperation,
    pub(super) target: NextOperation,
    pub(super) output: Option<DynOutput>,
    pub(super) tag: Option<String>,
}

pub(super) type ConnectVisitor<'a> = dyn Fn(
        &Vertex,
        &mut Builder,
        &DiagramElementRegistry,
        &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode>
    + 'a;

pub(super) struct Vertex {
    pub(super) in_edges: Vec<Arc<Mutex<Edge>>>,
    pub(super) out_edges: Vec<Arc<Mutex<Edge>>>,
}

struct VertexOperations<'a> {
    connect_visitor: Box<ConnectVisitor<'a>>,
}

pub(super) struct WorkflowBuilder<'a> {
    vertices: HashMap<OperationId, (RefCell<Vertex>, VertexOperations<'a>)>,
}

impl<'a> WorkflowBuilder<'a> {
    pub(super) fn new() -> Self {
        Self {
            vertices: HashMap::new(),
        }
    }

    pub(super) fn add_vertex<AsId, F>(&mut self, op_id: AsId, connect_visitor: F)
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
        self.vertices.insert(
            op_id.into_operation_id(),
            (
                RefCell::new(Vertex {
                    in_edges: Vec::new(),
                    out_edges: Vec::new(),
                }),
                VertexOperations {
                    connect_visitor: Box::new(connect_visitor),
                },
            ),
        );
    }

    pub(super) fn add_edge(&mut self, edge: Edge) -> Result<(), DiagramErrorCode> {
        let (from_vertex, _) = self
            .vertices
            .get(&edge.source.clone().into_operation_id())
            .ok_or_else(|| DiagramErrorCode::OperationNotFound(edge.source.to_string()))?;
        let (to_vertex, _) = self
            .vertices
            .get(&edge.target.clone().into_operation_id())
            .ok_or_else(|| DiagramErrorCode::OperationNotFound(edge.target.to_string()))?;

        let edge = Arc::new(Mutex::new(edge));
        from_vertex.borrow_mut().out_edges.push(Arc::clone(&edge));
        to_vertex.borrow_mut().in_edges.push(Arc::clone(&edge));

        Ok(())
    }

    pub(super) fn create_workflow(
        mut self,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
    ) -> Result<(), DiagramError> {
        let mut buffers = HashMap::new();

        // iteratively connect the vertices
        while !self.vertices.is_empty() {
            let before_len = self.vertices.len();
            let mut unconnected_vertices = HashMap::new();

            for (op_id, (mut v, mut ops)) in self.vertices.drain() {
                let vertex = v.get_mut();

                // skip if one or more edges is not ready yet
                if vertex
                    .in_edges
                    .iter()
                    .any(|edge| edge.try_lock().unwrap().output.is_none())
                {
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
        Ok(vertex.in_edges[0]
            .try_lock()
            .unwrap()
            .output
            .take()
            .unwrap())
    }
}
