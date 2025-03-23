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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Accessor, BufferSettings, Builder, JsonMessage};

use super::{
    type_info::TypeInfo, BufferInputs, BuildDiagramOperation, BuildStatus, DiagramContext,
    DiagramErrorCode, NextOperation, OperationId,
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
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let message_info = if self.serialize.is_some_and(|v| v) {
            TypeInfo::of::<JsonMessage>()
        } else {
            let Some(sample_input) = ctx.get_sample_output_into_target(id) else {
                // There are no outputs ready for this target, so we can't do
                // anything yet. The builder should try again later.

                // TODO(@mxgrey): We should allow users to explicitly specify the
                // message type for the buffer. When they do, we won't need to wait
                // for an input.
                return Ok(BuildStatus::defer("waiting for an input"));
            };

            *sample_input.message_info()
        };

        let buffer =
            ctx.registry
                .messages
                .create_buffer(&message_info, self.settings.clone(), builder)?;
        ctx.set_buffer_for_operation(id, buffer)?;
        Ok(BuildStatus::Finished)
    }
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

impl BuildDiagramOperation for BufferAccessSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        let target_type = ctx.get_node_request_type(self.target_node.as_ref(), &self.next)?;
        let node = ctx
            .registry
            .messages
            .with_buffer_access(&target_type, &buffer_map, builder)?;
        ctx.set_input_for_target(id, node.input)?;
        ctx.add_output_into_target(self.next.clone(), node.output);
        Ok(BuildStatus::Finished)
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

impl BuildDiagramOperation for ListenSchema {
    fn build_diagram_operation(
        &self,
        _: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        let target_type = ctx.get_node_request_type(self.target_node.as_ref(), &self.next)?;
        let output = ctx
            .registry
            .messages
            .listen(&target_type, &buffer_map, builder)?;
        ctx.add_output_into_target(self.next.clone(), output);
        Ok(BuildStatus::Finished)
    }
}

#[cfg(test)]
mod tests {
    use bevy_ecs::{prelude::World, system::In};
    use serde_json::json;

    use crate::{
        diagram::testing::DiagramTestFixture, Accessor, AnyBufferKey, AnyBufferWorldAccess,
        BufferAccess, BufferAccessMut, BufferKey, BufferWorldAccess, Diagram, DiagramErrorCode,
        IntoBlockingCallback, JsonBufferKey, JsonBufferWorldAccess, JsonMessage, JsonPosition,
        Node, NodeBuilderOptions,
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
                builder.create_map_block(|(_, msg): (JsonPosition, JsonMessage)| {
                    msg.as_number().unwrap().as_i64().unwrap()
                })
            },
        );

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("json_split_string"),
            |builder, _config: ()| {
                builder.create_map_block(|(_, msg): (JsonPosition, JsonMessage)| {
                    msg.as_str().unwrap().to_owned()
                })
            },
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

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
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
            .register_node_builder(NodeBuilderOptions::new("trigger"), |builder, _: ()| {
                builder.create_map_block(|_: JsonMessage| ())
            });

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("check_for_all"),
                move |builder, _config: ()| {
                    let expected = expected.clone();
                    builder.create_node(
                        (move |In((_, keys)): In<((), TestAccessor)>, world: &mut World| {
                            wait_for_all(keys, world, &expected)
                        })
                        .into_blocking_callback(),
                    )
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
                    builder.create_node(
                        (move |In(keys): In<TestAccessor>, world: &mut World| {
                            wait_for_all(keys, world, &expected)
                        })
                        .into_blocking_callback(),
                    )
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
