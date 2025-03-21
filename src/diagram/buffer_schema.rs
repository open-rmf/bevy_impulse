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

use std::collections::{hash_map::Entry, HashMap};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    unknown_diagram_error, Accessor, AnyBuffer, BufferIdentifier, BufferMap, BufferSettings,
    Builder, InputSlot, Output, JsonMessage,
};

use super::{
    type_info::TypeInfo,
    workflow_builder::{Edge, EdgeBuilder, Vertex},
    BuildDiagramOperation, BuildStatus, DiagramContext,
    BuiltinTarget, Diagram, DiagramElementRegistry, DiagramErrorCode, DiagramOperation, DynOutput,
    MessageRegistry, NextOperation, OperationId, BufferInputs,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BufferSchema {
    #[serde(default)]
    pub(super) settings: BufferSettings,

    /// If true, messages will be serialized before sending into the buffer.
    pub(super) serialize: Option<bool>,
}

impl BuildDiagramOperation for BufferSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let message_info = if self.serialize.is_some_and(|v| v) {
            TypeInfo::of::<JsonMessage>()
        } else {
            let Some(sample_input) = ctx.construction.get_sample_output_into_target(id) else {
                // There are no outputs ready for this target, so we can't do
                // anything yet. The builder should try again later.

                // TODO(@mxgrey): We should allow users to explicitly specify the
                // message type for the buffer. When they do, we won't need to wait
                // for an input.
                return Ok(BuildStatus::defer("waiting for an input"));
            };

            *sample_input.message_info()
        };


    }
}


impl BufferSchema {
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
        let buffer = first_output.into_any_buffer(builder, self.settings)?;
        let buffer = match buffers.entry(vertex.op_id) {
            Entry::Occupied(mut entry) => {
                entry.insert(buffer);
                entry.into_mut()
            }
            Entry::Vacant(entry) => entry.insert(buffer),
        };

        // connect the rest of the outputs to the buffer
        for output in rest_outputs {
            buffer.receive_output(builder, output)?;
        }

        Ok(true)
    }
}

/// if `target` has a value, return the request type of the operation, else, return the request type of `next`
pub(super) fn get_node_request_type(
    target: &Option<OperationId>,
    next: &NextOperation,
    diagram: &Diagram,
    registry: &DiagramElementRegistry,
) -> Result<TypeInfo, DiagramErrorCode> {
    let target_node = if let Some(target) = target {
        diagram.get_op(target)?
    } else {
        match next {
            NextOperation::Target(op_id) => diagram.get_op(op_id)?,
            NextOperation::Builtin { builtin } => match builtin {
                BuiltinTarget::Terminate => return Ok(TypeInfo::of::<serde_json::Value>()),
                _ => return Err(DiagramErrorCode::UnknownTarget),
            },
        }
    };
    let node_op = match target_node {
        DiagramOperation::Node(op) => op,
        _ => return Err(DiagramErrorCode::UnknownTarget),
    };
    let target_type = registry.get_node_registration(&node_op.builder)?.request;
    Ok(target_type)
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BufferAccessSchema {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this operation is for. The id must be a `node` operation. Optional if `next` is a node operation.
    pub(super) target_node: Option<OperationId>,
}

impl BufferAccessSchema {
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

        let target_type = get_node_request_type(&self.target_node, &self.next, diagram, registry)?;
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

pub trait BufferAccessRequest {
    type Message: Send + Sync + 'static;
    type BufferKeys: Accessor;
}

impl<T, B> BufferAccessRequest for (T, B)
where
    T: Send + Sync + 'static,
    B: Accessor,
{
    type Message = T;
    type BufferKeys = B;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ListenSchema {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this operation is for. The id must be a `node` operation. Optional if `next` is a node operation.
    pub(super) target_node: Option<OperationId>,
}

impl ListenSchema {
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

        let target_type = get_node_request_type(&self.target_node, &self.next, diagram, registry)?;
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
    use bevy_ecs::{prelude::World, system::In};
    use serde_json::json;

    use crate::{
        diagram::testing::DiagramTestFixture, Accessor, AnyBufferKey, AnyBufferWorldAccess, BufferAccess,
        BufferAccessMut, BufferKey, BufferWorldAccess, Diagram, DiagramErrorCode, IntoBlockingCallback, JsonBufferKey,
        JsonBufferWorldAccess, JsonMessage, JsonPosition, Node, NodeBuilderOptions,
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
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("insert_json_buffer_entries".to_owned()),
                |builder, config: usize| {
                    builder.create_node(
                        (move |In((value, key)): In<(JsonMessage, JsonBufferKey)>,
                               world: &mut World| {
                            world
                                .json_buffer_mut(&key, |mut buffer| {
                                    for _ in 0..config {
                                        buffer.push(value.clone()).unwrap();
                                    }
                                })
                                .unwrap();
                        })
                        .into_blocking_callback(),
                    )
                },
            )
            .with_buffer_access()
            .with_common_response();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("count_json_buffer_entries".to_owned()),
                |builder, _config: ()| {
                    builder.create_node(count_json_buffer_entries.into_blocking_callback())
                },
            )
            .with_buffer_access()
            .with_common_response();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_count_json_buffer_entries".to_owned()),
                |builder, _config: ()| {
                    builder.create_node(listen_count_json_buffer_entries.into_blocking_callback())
                },
            )
            .with_listen()
            .with_common_response();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("count_any_buffer_entries".to_owned()),
                |builder, _config: ()| {
                    builder.create_node(count_any_buffer_entries.into_blocking_callback())
                },
            )
            .with_buffer_access()
            .with_common_response();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_count_any_buffer_entries".to_owned()),
                |builder, _config: ()| {
                    builder.create_node(listen_count_any_buffer_entries.into_blocking_callback())
                },
            )
            .with_listen()
            .with_common_response();

        // TODO(@mxgrey): Replace these with a general deserializing operation
        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("deserialize_i64"),
            |builder, _config: ()| {
                builder
                    .create_map_block(|msg: JsonMessage| msg.as_number().unwrap().as_i64().unwrap())
            },
        );

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("json_split_i64"),
            |builder, _config: ()| {
                builder.create_map_block(|(_, msg): (JsonPosition, JsonMessage)| msg.as_number().unwrap().as_i64().unwrap())
            }
        );

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("json_split_string"),
            |builder, _config: ()| {
                builder.create_map_block(|(_, msg): (JsonPosition, JsonMessage)| msg.as_str().unwrap().to_owned())
            }
        );

        fixture
    }

    fn count_json_buffer_entries(
        In(((), key)): In<((), JsonBufferKey)>,
        world: &mut World,
    ) -> usize {
        world.json_buffer_view(&key).unwrap().len()
    }

    fn listen_count_json_buffer_entries(In(key): In<JsonBufferKey>, world: &mut World) -> usize {
        world.json_buffer_view(&key).unwrap().len()
    }

    fn count_any_buffer_entries(In(((), key)): In<((), AnyBufferKey)>, world: &mut World) -> usize {
        world.any_buffer_view(&key).unwrap().len()
    }

    fn listen_count_any_buffer_entries(In(key): In<AnyBufferKey>, world: &mut World) -> usize {
        world.any_buffer_view(&key).unwrap().len()
    }

    #[test]
    fn test_buffer_mismatch_type() {
        let mut fixture = new_fixture();
        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("join_i64"),
                |builder, _config: ()| builder.create_map_block(|i: Vec<i64>| i[0]),
            )
            .with_join();

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
                },
                "join": {
                    "type": "join",
                    "buffers": ["buffer"],
                    "target_node": "op1",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "join_i64",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::IncompatibleBuffers(_)),
            "{:#?}",
            err
        );
    }

    #[test]
    fn test_buffer_multiple_inputs() {
        let mut fixture = new_fixture();
        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("wait_2_strings"),
                |builder, _config: ()| {
                    let n = builder.create_node(
                        (|In(req): In<Vec<BufferKey<String>>>, access: BufferAccess<String>| {
                            if access.get(&req[0]).unwrap().len() < 2 {
                                None
                            } else {
                                Some("hello world".to_string())
                            }
                        })
                        .into_blocking_callback(),
                    );
                    let output = n.output.chain(builder).dispose_on_none().output();
                    Node::<Vec<BufferKey<String>>, String> {
                        input: n.input,
                        output,
                        streams: n.streams,
                    }
                },
            )
            .with_listen();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["string_output", "string_output2"],
                },
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "string_output2": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                    "settings": {
                        "retention": "keep_all",
                    },
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "target_node": "wait_2_strings",
                    "next": "wait_2_strings",
                },
                "wait_2_strings": {
                    "type": "node",
                    "builder": "wait_2_strings",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_buffer_access() {
        let mut fixture = new_fixture();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("with_buffer_access"),
                |builder, _config: ()| {
                    builder.create_map_block(|req: (i64, Vec<BufferKey<String>>)| req.0)
                },
            )
            .with_buffer_access();

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
                    "next": "buffer_access",
                },
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "string_buffer",
                },
                "string_buffer": {
                    "type": "buffer",
                },
                "buffer_access": {
                    "type": "buffer_access",
                    "buffers": ["string_buffer"],
                    "target_node": "with_buffer_access",
                    "next": "with_buffer_access",
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
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 1);
    }

    #[test]
    fn test_json_buffer_access() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_input",
            "ops": {
                "fork_input": {
                    "type": "fork_clone",
                    "next": [
                        "json_buffer",
                        "insert_access",
                    ],
                },
                "json_buffer": {
                    "type": "buffer",
                    "settings": { "retention": "keep_all" },
                },
                "insert_access": {
                    "type": "buffer_access",
                    "buffers": ["json_buffer"],
                    "next": "insert",
                },
                "insert": {
                    "type": "node",
                    "builder": "insert_json_buffer_entries",
                    "config": 10,
                    "next": "count_access",
                },
                "count_access": {
                    "type": "buffer_access",
                    "buffers": "json_buffer",
                    "next": "count",
                },
                "count": {
                    "type": "node",
                    "builder": "count_json_buffer_entries",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::String("hello".to_owned()))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 11);
    }

    #[test]
    fn test_any_buffer_access() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_input",
            "ops": {
                "fork_input": {
                    "type": "fork_clone",
                    "next": [
                        "json_buffer",
                        "insert_access",
                    ],
                },
                "json_buffer": {
                    "type": "buffer",
                    "settings": { "retention": "keep_all" },
                },
                "insert_access": {
                    "type": "buffer_access",
                    "buffers": ["json_buffer"],
                    "next": "insert",
                },
                "insert": {
                    "type": "node",
                    "builder": "insert_json_buffer_entries",
                    "config": 10,
                    "next": "count_access",
                },
                "count_access": {
                    "type": "buffer_access",
                    "buffers": "json_buffer",
                    "next": "count",
                },
                "count": {
                    "type": "node",
                    "builder": "count_any_buffer_entries",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::String("hello".to_owned()))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 11);
    }

    #[test]
    fn test_generic_listen() {
        let mut fixture = new_fixture();

        fn count_generic_buffer(
            In(key): In<BufferKey<i64>>,
            mut access: BufferAccessMut<i64>,
        ) -> i64 {
            access.get_mut(&key).unwrap().pull().unwrap()
        }

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("pull_generic_buffer"),
                |builder, _config: ()| {
                    builder.create_node(count_generic_buffer.into_blocking_callback())
                },
            )
            .with_listen()
            .with_common_response();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "deserialize",
            "ops": {
                "deserialize": {
                    "type": "node",
                    "builder": "deserialize_i64",
                    "next": "buffer",
                },
                "buffer": { "type": "buffer" },
                "listen": {
                    "type": "listen",
                    "buffers": "buffer",
                    "next": "count",
                },
                "count": {
                    "type": "node",
                    "builder": "pull_generic_buffer",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, JsonMessage::Number(5_i64.into()))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 5_i64);
    }

    #[test]
    fn test_vec_listen() {
        let mut fixture = new_fixture();

        fn listen_buffer(In(request): In<Vec<BufferKey<i64>>>, access: BufferAccess<i64>) -> usize {
            access.get(&request[0]).unwrap().len()
        }

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_buffer"),
                |builder, _config: ()| -> Node<Vec<BufferKey<i64>>, usize, ()> {
                    builder.create_node(listen_buffer.into_blocking_callback())
                },
            )
            .with_listen()
            .with_common_response();

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
                    "target_node": "listen_buffer",
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

        let result = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 1);
    }

    #[test]
    fn test_json_buffer_listen() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": { "type": "buffer" },
                "listen": {
                    "type": "listen",
                    "buffers": "buffer",
                    "next": "count",
                },
                "count": {
                    "type": "node",
                    "builder": "listen_count_json_buffer_entries",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 1);
    }

    #[test]
    fn test_any_buffer_listen() {
        let mut fixture = new_fixture();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": { "type": "buffer" },
                "listen": {
                    "type": "listen",
                    "buffers": "buffer",
                    "next": "count",
                },
                "count": {
                    "type": "node",
                    "builder": "listen_count_any_buffer_entries",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 1);
    }

    #[derive(Accessor, Clone)]
    struct TestAccessor {
        integer: BufferKey<i64>,
        string: BufferKey<String>,
        json: JsonBufferKey,
        any: AnyBufferKey,
    }

    #[test]
    fn test_struct_accessor_access() {
        let mut fixture = new_fixture();

        let input = JsonMessage::Object(serde_json::Map::from_iter([
            ("integer".to_owned(), JsonMessage::Number(5_i64.into())),
            ("string".to_owned(), JsonMessage::String("hello".to_owned())),
        ]));

        let expected = input.clone();

        // TODO(@mxgrey): Replace this with a builtin trigger operation
        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("trigger"),
                |builder, _: ()| {
                    builder.create_map_block(|_: JsonMessage| ())
                }
            );

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("check_for_all"),
                move |builder, _config: ()| {
                    let expected = expected.clone();
                    builder.create_node((
                    move |In((_, keys)): In<((), TestAccessor)>, world: &mut World| {
                        wait_for_all(keys, world, &expected)
                    }).into_blocking_callback())
                },
            )
            .with_buffer_access()
            .with_fork_result()
            .with_common_response();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_clone",
                    "next": [
                        "split",
                        "json_buffer",
                        "any_buffer",
                        "trigger",
                    ],
                },
                "split": {
                    "type": "split",
                    "keyed": {
                        "integer": "push_integer",
                        "string": "push_string",
                    },
                },
                "push_integer": {
                    "type": "node",
                    "builder": "json_split_i64",
                    "next": "integer_buffer",
                },
                "push_string": {
                    "type": "node",
                    "builder": "json_split_string",
                    "next": "string_buffer",
                },
                "integer_buffer": { "type": "buffer" },
                "string_buffer": { "type": "buffer" },
                "json_buffer": { "type": "buffer" },
                "any_buffer": { "type": "buffer" },
                "trigger": {
                    "type": "node",
                    "builder": "trigger",
                    "next": "access",
                },
                "access": {
                    "type": "buffer_access",
                    "buffers": {
                        "integer": "integer_buffer",
                        "string": "string_buffer",
                        "json": "json_buffer",
                        "any": "any_buffer",
                    },
                    "next": "check_for_all"
                },
                "check_for_all": {
                    "type": "node",
                    "builder": "check_for_all",
                    "next": "filter",
                },
                "filter": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": "access",
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_and_run(&diagram, input).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, JsonMessage::Null);
    }

    #[test]
    fn test_struct_accessor_listen() {
        let mut fixture = new_fixture();

        let input = JsonMessage::Object(serde_json::Map::from_iter([
            ("integer".to_owned(), JsonMessage::Number(5_i64.into())),
            ("string".to_owned(), JsonMessage::String("hello".to_owned())),
        ]));

        let expected = input.clone();

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_for_all"),
                move |builder, _config: ()| {
                    let expected = expected.clone();
                    builder.create_node((
                    move |In(keys): In<TestAccessor>, world: &mut World| {
                        wait_for_all(keys, world, &expected)
                    }).into_blocking_callback())
                },
            )
            .with_listen()
            .with_fork_result()
            .with_common_response();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_clone",
                    "next": [
                        "split",
                        "json_buffer",
                        "any_buffer",
                    ],
                },
                "split": {
                    "type": "split",
                    "keyed": {
                        "integer": "push_integer",
                        "string": "push_string",
                    },
                },
                "push_integer": {
                    "type": "node",
                    "builder": "json_split_i64",
                    "next": "integer_buffer",
                },
                "push_string": {
                    "type": "node",
                    "builder": "json_split_string",
                    "next": "string_buffer",
                },
                "integer_buffer": { "type": "buffer" },
                "string_buffer": { "type": "buffer" },
                "json_buffer": { "type": "buffer" },
                "any_buffer": { "type": "buffer" },
                "listen": {
                    "type": "listen",
                    "buffers": {
                        "integer": "integer_buffer",
                        "string": "string_buffer",
                        "json": "json_buffer",
                        "any": "any_buffer",
                    },
                    "next": "listen_for_all"
                },
                "listen_for_all": {
                    "type": "node",
                    "builder": "listen_for_all",
                    "next": "filter",
                },
                "filter": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin": "dispose" },
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_and_run(&diagram, input).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, JsonMessage::Null);
    }

    fn wait_for_all(
        keys: TestAccessor,
        world: &mut World,
        expected: &JsonMessage,
    ) -> Result<(), ()> {
        if let Some(integer) = world.buffer_view(&keys.integer).unwrap().newest() {
            assert_eq!(*integer, 5);
        } else {
            return Err(());
        }

        if let Some(string) = world.buffer_view(&keys.string).unwrap().newest() {
            assert_eq!(string, "hello");
        } else {
            return Err(());
        }

        if let Ok(Some(json)) = world.json_buffer_view(&keys.json).unwrap().newest() {
            assert_eq!(&json, expected);
        } else {
            return Err(());
        }

        if let Some(any) = world.any_buffer_view(&keys.any).unwrap().newest() {
            assert_eq!(any.downcast_ref::<JsonMessage>().unwrap(), expected);
        } else {
            return Err(());
        }

        Ok(())
    }
}
