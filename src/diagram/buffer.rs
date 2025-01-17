use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    unknown_diagram_error, Accessible, Buffer, BufferSettings, Builder, IterBufferable, Joinable,
};

use super::{
    workflow_builder::{Edge, EdgeBuilder, Vertex},
    BoxedMessage, DiagramErrorCode, MessageRegistry, NextOperation, OperationId, SourceOperation,
};

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
pub struct BufferOp {
    #[serde(default = "BufferOp::default_buffer_settings")]
    pub(super) settings: BufferSettings,
    pub(super) next: Option<NextOperation>,
}

impl BufferOp {
    fn default_buffer_settings() -> BufferSettings {
        BufferSettings::keep_all()
    }
}

impl BufferOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(next) = &self.next {
            builder.add_output_edge(&next, None)?;
        }
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        builder: &mut Builder,
        vertex: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        registry: &MessageRegistry,
        buffers: &HashMap<&OperationId, Buffer<BoxedMessage>>,
    ) -> Result<bool, DiagramErrorCode> {
        // all buffers are `Buffer<BoxedMessage>`, to connect a buffer,
        // 1. box the incoming output
        // 2. connect the boxed output to the buffer input
        // 3. unbox the output, normally a `BoxedMessage` erases the type so it is not possible
        //    to unbox, but because we have the original incoming output, we can unbox it.
        // 4. store the unboxed output in the output edge.

        if vertex.in_edges.is_empty() {
            // output connection is optional for buffers
            return Ok(true);
        }

        if vertex.in_edges.len() != 1 {
            return Err(DiagramErrorCode::OnlySingleInput);
        }

        let output = if let Some(output) = edges
            .get_mut(&vertex.in_edges[0])
            .ok_or_else(|| unknown_diagram_error!())?
            .output
            .take()
        {
            output
        } else {
            return Ok(false);
        };

        let buffer = buffers
            .get(vertex.op_id)
            .ok_or_else(|| DiagramErrorCode::OperationNotFound(vertex.op_id.clone()))?;
        let output_type_id = output.type_info;
        let boxed_output = registry.boxed(builder, output)?;
        builder.connect(boxed_output, buffer.input_slot());

        if let Some(out_edge_id) = vertex.out_edges.get(0) {
            let out_edge = edges.get_mut(out_edge_id).ok_or(unknown_diagram_error!())?;
            let op_id = match &out_edge.source {
                SourceOperation::Source(op_id) => op_id,
                _ => panic!(),
            };
            let buffer = buffers[op_id];
            let buffer_output = buffer.join(builder).output();
            let unboxed_output = registry.unbox(builder, buffer_output.into(), &output_type_id)?;
            out_edge.output = Some(unboxed_output);
        }
        Ok(true)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
#[serde(rename_all = "snake_case")]
pub struct BufferAccessOp {
    pub(super) buffers: Vec<OperationId>,
    pub(super) next: NextOperation,
}

impl BufferAccessOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(&self.next, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        builder: &mut Builder,
        vertex: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        registry: &MessageRegistry,
        buffers: &HashMap<&OperationId, Buffer<BoxedMessage>>,
    ) -> Result<bool, DiagramErrorCode> {
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

        let buffers = self
            .buffers
            .iter()
            .map(|op_id| -> Result<_, DiagramErrorCode> {
                Ok(buffers
                    .get(op_id)
                    .ok_or_else(|| DiagramErrorCode::OperationNotFound(op_id.clone()))?
                    .clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = registry.with_buffer_access(builder, output, buffers)?;
        let out_edge = edges
            .get_mut(
                vertex
                    .out_edges
                    .get(0)
                    .ok_or_else(|| unknown_diagram_error!())?,
            )
            .ok_or(unknown_diagram_error!())?;
        out_edge.output = Some(output);
        Ok(true)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
#[serde(rename_all = "snake_case")]
pub struct ListenOp {
    pub(super) buffers: Vec<OperationId>,
    pub(super) next: NextOperation,
}

impl ListenOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut edge_builder: EdgeBuilder<'a, '_>,
        builder: &mut Builder,
        buffers: &HashMap<&'a OperationId, Buffer<BoxedMessage>>,
    ) -> Result<(), DiagramErrorCode> {
        let listen_buffers = self
            .buffers
            .iter()
            .map(|op_id| -> Result<_, DiagramErrorCode> {
                Ok(buffers
                    .get(&op_id)
                    .ok_or_else(|| DiagramErrorCode::OperationNotFound(op_id.clone()))?
                    .clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let joined_buffers_output = listen_buffers.join_vec::<4>(builder).output();
        edge_builder.add_output_edge(
            &self.next,
            Some(joined_buffers_output.listen(builder).output().into()),
        )?;
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
