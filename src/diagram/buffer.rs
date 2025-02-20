use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    unknown_diagram_error, AnyBuffer, BufferIdentifier, BufferKeyMap, BufferMap, BufferSettings,
    Builder, InputSlot, Output,
};

use super::{
    type_info::TypeInfo,
    workflow_builder::{Edge, EdgeBuilder, Vertex},
    Diagram, DiagramElementRegistry, DiagramErrorCode, DiagramOperation, DynOutput,
    MessageRegistry, NextOperation, OperationId,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BufferOp {
    #[serde(default = "BufferOp::default_buffer_settings")]
    pub(super) settings: BufferSettings,

    /// If true, messages will be serialized before sending into the buffer.
    pub(super) serialize: Option<bool>,
}

impl BufferOp {
    fn default_buffer_settings() -> BufferSettings {
        BufferSettings::keep_all()
    }
}

impl BufferOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        _builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        Ok(())
    }

    pub(super) fn try_connect<'a>(
        &self,
        builder: &mut Builder,
        vertex: &'a Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        buffers: &mut HashMap<&'a OperationId, AnyBuffer>,
        registry: &MessageRegistry,
    ) -> Result<bool, DiagramErrorCode> {
        if vertex.in_edges.is_empty() {
            // this will eventually cause workflow builder to return a [`DiagramErrorCode::IncompleteDiagram`] error.
            return Ok(false);
        }

        let first_output = edges
            // SAFETY: we checked that `vertex.in_edges` is not empty.
            .remove(&vertex.in_edges[0])
            .ok_or_else(|| unknown_diagram_error!())?
            .output
            .take()
            // expected all inputs to be ready
            .ok_or_else(|| unknown_diagram_error!())?;
        let first_output = if self.serialize.unwrap_or(false) {
            registry.serialize(builder, first_output)?.into()
        } else {
            first_output
        };

        let rest_outputs: Vec<DynOutput> = vertex.in_edges[1..]
            .iter()
            .map(|edge_id| -> Result<_, DiagramErrorCode> {
                let output = edges
                    .remove(edge_id)
                    .ok_or_else(|| unknown_diagram_error!())?
                    .output
                    .take()
                    // expected all inputs to be ready
                    .ok_or_else(|| unknown_diagram_error!())?;
                if self.serialize.unwrap_or(false) {
                    Ok(registry.serialize(builder, output)?.into())
                } else {
                    Ok(output)
                }
            })
            .collect::<Result<_, _>>()?;

        // check that all inputs are the same type
        let expected_type = first_output.type_info;
        for output in &rest_outputs {
            if output.type_info != expected_type {
                return Err(DiagramErrorCode::TypeMismatch {
                    source_type: output.type_info,
                    target_type: expected_type,
                });
            }
        }

        // convert the first output into a buffer
        let entry = buffers
            .entry(vertex.op_id)
            .insert_entry(first_output.into_any_buffer(builder)?);
        let buffer = entry.get();

        // connect the rest of the outputs to the buffer
        for output in rest_outputs {
            buffer.receive_output(builder, output)?;
        }

        Ok(true)
    }
}

pub(super) fn get_node_request_type(
    target_node: &OperationId,
    diagram: &Diagram,
    registry: &DiagramElementRegistry,
) -> Result<TypeInfo, DiagramErrorCode> {
    let target_node = diagram
        .ops
        .get(target_node)
        .ok_or_else(|| DiagramErrorCode::OperationNotFound(target_node.clone()))?;
    let target_node_op = match target_node {
        DiagramOperation::Node(op) => op,
        _ => {
            return Err(DiagramErrorCode::UnexpectedOperationType {
                expected: "node".to_string(),
                got: target_node.to_string(),
            })
        }
    };
    let target_type = registry
        .get_node_registration(&target_node_op.builder)?
        .request;
    Ok(target_type)
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", untagged)]
pub enum BufferInputs {
    Dict(HashMap<String, OperationId>),
    Array(Vec<OperationId>),
}

impl BufferInputs {
    /// Creates a [`BufferMap`] from the buffer inputs.
    /// Returns `None` if one or more buffer does not exist.
    pub(super) fn as_buffer_map(
        &self,
        buffers: &HashMap<&OperationId, AnyBuffer>,
    ) -> Option<BufferMap> {
        match self {
            Self::Dict(mapping) => {
                let mut buffer_map = BufferMap::with_capacity(mapping.len() * 2);
                for (k, op_id) in mapping {
                    let buffer = if let Some(buffer) = buffers.get(op_id) {
                        buffer
                    } else {
                        return None;
                    };
                    buffer_map.insert(BufferIdentifier::Name(k.clone().into()), *buffer);
                }
                Some(buffer_map)
            }
            Self::Array(arr) => {
                let mut buffer_map = BufferMap::with_capacity(arr.len() * 2);
                for (i, op_id) in arr.into_iter().enumerate() {
                    let buffer = if let Some(buffer) = buffers.get(op_id) {
                        buffer
                    } else {
                        return None;
                    };
                    buffer_map.insert(BufferIdentifier::Index(i), *buffer);
                }
                Some(buffer_map)
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Dict(d) => d.is_empty(),
            Self::Array(a) => a.is_empty(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BufferAccessOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this buffer access is for. The id must be a `node` operation.
    pub(super) target_node: OperationId,
}

impl BufferAccessOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(&self.next, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'a>(
        &self,
        builder: &mut Builder,
        vertex: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        registry: &DiagramElementRegistry,
        buffers: &HashMap<&OperationId, AnyBuffer>,
        diagram: &Diagram,
    ) -> Result<bool, DiagramErrorCode> {
        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };

        let output = if let Some(output) = edges
            .get_mut(
                vertex
                    .in_edges
                    .get(0)
                    .ok_or_else(|| unknown_diagram_error!())?,
            )
            .ok_or_else(|| unknown_diagram_error!())?
            .output
            .take()
        {
            output
        } else {
            return Ok(false);
        };

        let target_type = get_node_request_type(&self.target_node, diagram, registry)?;
        let output =
            registry
                .messages
                .with_buffer_access(builder, output, &buffers, target_type)?;
        let out_edge = edges
            .get_mut(
                vertex
                    .out_edges
                    .get(0)
                    .ok_or_else(|| unknown_diagram_error!())?,
            )
            .ok_or_else(|| unknown_diagram_error!())?;
        out_edge.output = Some(output);
        Ok(true)
    }
}

pub(super) trait BufferAccessRequest {
    type Message: Send + Sync + 'static;
    type BufferKeys: BufferKeyMap;
}

impl<T, B> BufferAccessRequest for (T, B)
where
    T: Send + Sync + 'static,
    B: BufferKeyMap,
{
    type Message = T;
    type BufferKeys = B;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ListenOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this operation is for. The id must be a `node` operation.
    pub(super) target_node: OperationId,
}

impl ListenOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(&self.next, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'a>(
        &self,
        builder: &mut Builder,
        vertex: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        registry: &DiagramElementRegistry,
        buffers: &HashMap<&OperationId, AnyBuffer>,
        diagram: &Diagram,
    ) -> Result<bool, DiagramErrorCode> {
        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };

        let target_type = get_node_request_type(&self.target_node, diagram, registry)?;
        let output = registry.messages.listen(builder, &buffers, target_type)?;
        let out_edge = edges
            .get_mut(
                vertex
                    .out_edges
                    .get(0)
                    .ok_or_else(|| unknown_diagram_error!())?,
            )
            .ok_or_else(|| unknown_diagram_error!())?;
        out_edge.output = Some(output);
        Ok(true)
    }
}

trait ReceiveOutput {
    fn receive_output(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(), DiagramErrorCode>;
}

impl ReceiveOutput for AnyBuffer {
    fn receive_output(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(), DiagramErrorCode> {
        if self.message_type_id() != output.type_info.type_id {
            return Err(DiagramErrorCode::TypeMismatch {
                source_type: TypeInfo {
                    type_id: self.message_type_id(),
                    type_name: self.message_type_name(),
                },
                target_type: output.type_info,
            });
        }
        // FIXME(koonpeng): This can potentially cause the workflow to run forever and never halt.
        // For now this works because neither of these operations creates or use any bevy Components.
        let input_slot = InputSlot::<()>::new(self.location.scope, self.location.source);
        let output = Output::<()>::new(output.scope(), output.id());
        builder.connect(output, input_slot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        diagram::{testing::DiagramTestFixture, BoxedMessage},
        BufferKeys, Diagram, DiagramError, DiagramErrorCode, JoinOutput, NodeBuilderOptions,
        Output,
    };

    /// create a new [`DiagramTestFixture`] with some extra builders.
    fn new_fixture() -> DiagramTestFixture {
        let mut fixture = DiagramTestFixture::new();

        fn num_output(_: serde_json::Value) -> i64 {
            1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("num_output".to_string()),
            |builder, _config: ()| builder.create_map_block(num_output),
        );

        fn string_output(_: serde_json::Value) -> String {
            "hello".to_string()
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("string_output".to_string()),
            |builder, _config: ()| builder.create_map_block(string_output),
        );

        fixture
    }

    #[test]
    fn test_buffer() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": {
                    "type": "buffer",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 12);
    }

    #[test]
    fn test_buffer_into_fork_clone() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": {
                    "type": "buffer",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": [{ "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 4);
    }

    #[test]
    fn test_buffer_mismatch_type() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "string_output",
            "ops": {
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(
                err.code,
                DiagramErrorCode::TypeMismatch {
                    target_type: _,
                    source_type: _
                }
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_buffer_multiple_inputs() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["num_output", "string_output"],
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer",
                },
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err()
            .downcast::<DiagramError>()
            .unwrap();
        assert!(
            matches!(err.code, DiagramErrorCode::OnlySingleInput),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_buffer_into_join() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["num_output", "num_output2"],
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                    "next": "join",
                },
                "num_output2": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "join",
                },
                "join": {
                    "type": "join",
                    "inputs": ["buffer", "num_output2"],
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result[0], 1);
        assert_eq!(result[1], 1);
    }

    #[test]
    fn test_buffer_access() {
        let mut fixture = new_fixture();

        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("with_buffer_access"),
                |builder, _config: ()| {
                    builder.create_map_block(
                        |req: (i64, BufferKeys<Output<JoinOutput<BoxedMessage>>>)| req.0,
                    )
                },
            );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "num_output",
            "ops": {
                "buffer": {
                    "type": "buffer",
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer_access",
                },
                "buffer_access": {
                    "type": "buffer_access",
                    "buffers": ["buffer"],
                    "next": "with_buffer_access"
                },
                "with_buffer_access": {
                    "type": "node",
                    "builder": "with_buffer_access",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_listen() {
        let mut fixture = new_fixture();

        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_buffer"),
                |builder, _config: ()| {
                    builder.create_map_block(|_: BufferKeys<Output<JoinOutput<BoxedMessage>>>| 2)
                },
            );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "num_output",
            "ops": {
                "buffer": {
                    "type": "buffer",
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer",
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "next": "listen_buffer",
                },
                "listen_buffer": {
                    "type": "node",
                    "builder": "listen_buffer",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result, 2);
    }
}
