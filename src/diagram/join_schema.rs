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
use smallvec::SmallVec;

use crate::{Builder, JsonMessage};

use super::{
    BufferInputs, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    NextOperation, OperationName,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinSchema {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,
}

impl BuildDiagramOperation for JoinSchema {
    fn build_diagram_operation(
        &self,
        _: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let Some(target_type) = ctx.infer_input_type_into_target(&self.next)? else {
            return Ok(BuildStatus::defer("waiting to find out target message type"));
        };

        let buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        let output = ctx
            .registry
            .messages
            .join(&target_type, &buffer_map, builder)?;
        ctx.add_output_into_target(&self.next, output);
        Ok(BuildStatus::Finished)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SerializedJoinSchema {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,
}

impl BuildDiagramOperation for SerializedJoinSchema {
    fn build_diagram_operation(
        &self,
        _: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        let output = builder.try_join::<JsonMessage>(&buffer_map)?.output();
        ctx.add_output_into_target(&self.next, output.into());

        Ok(BuildStatus::Finished)
    }
}

/// The resulting type of a `join` operation. Nodes receiving a join output must have request
/// of this type. Note that the join output is NOT serializable. If you would like to serialize it,
/// convert it to a `Vec` first.
pub type JoinOutput<T> = SmallVec<[T; 4]>;

#[cfg(test)]
mod tests {
    use bevy_impulse_derive::Joined;
    use serde_json::json;
    use test_log::test;

    use super::*;
    use crate::{
        diagram::testing::DiagramTestFixture, Diagram, DiagramElementRegistry,
        DiagramErrorCode, NodeBuilderOptions,
    };

    fn foo(_: serde_json::Value) -> String {
        "foo".to_string()
    }

    fn bar(_: serde_json::Value) -> String {
        "bar".to_string()
    }

    #[derive(Serialize, Deserialize, JsonSchema, Joined)]
    struct FooBar {
        foo: String,
        bar: String,
    }

    impl Default for FooBar {
        fn default() -> Self {
            FooBar {
                foo: "foo".to_owned(),
                bar: "bar".to_owned(),
            }
        }
    }

    fn foobar(foobar: FooBar) -> String {
        format!("{}{}", foobar.foo, foobar.bar)
    }

    fn foobar_array(foobar: Vec<String>) -> String {
        format!("{}{}", foobar[0], foobar[1])
    }

    fn register_join_nodes(registry: &mut DiagramElementRegistry) {
        registry.register_node_builder(NodeBuilderOptions::new("foo"), |builder, _config: ()| {
            builder.create_map_block(foo)
        });
        registry.register_node_builder(NodeBuilderOptions::new("bar"), |builder, _config: ()| {
            builder.create_map_block(bar)
        });
        registry
            .opt_out()
            .no_cloning()
            .register_node_builder(NodeBuilderOptions::new("foobar"), |builder, _config: ()| {
                builder.create_map_block(foobar)
            })
            .with_join();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("foobar_array"),
                |builder, _config: ()| builder.create_map_block(foobar_array),
            )
            .with_join();
        registry.opt_out().no_cloning().register_node_builder(
            NodeBuilderOptions::new("create_foobar"),
            |builder, config: FooBar| {
                builder.create_map_block(move |_: JsonMessage| FooBar {
                    foo: config.foo.clone(),
                    bar: config.bar.clone(),
                })
            },
        );
    }

    #[test]
    fn test_join() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["foo", "bar"],
                },
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": "foo_buffer",
                },
                "foo_buffer": {
                    "type": "buffer",
                },
                "bar": {
                    "type": "node",
                    "builder": "bar",
                    "next": "bar_buffer",
                },
                "bar_buffer": {
                    "type": "buffer",
                },
                "join": {
                    "type": "join",
                    "buffers": {
                        "foo": "foo_buffer",
                        "bar": "bar_buffer",
                    },
                    "target_node": "foobar",
                    "next": "foobar",
                },
                "foobar": {
                    "type": "node",
                    "builder": "foobar",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "foobar");
    }

    #[test]
    /// similar to `test_join`, except the `target_node` field is not provided and the target type is inferred from `next`.
    fn test_join_infer_type() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["foo", "bar"],
                },
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": "foo_buffer",
                },
                "foo_buffer": {
                    "type": "buffer",
                },
                "bar": {
                    "type": "node",
                    "builder": "bar",
                    "next": "bar_buffer",
                },
                "bar_buffer": {
                    "type": "buffer",
                },
                "join": {
                    "type": "join",
                    "buffers": {
                        "foo": "foo_buffer",
                        "bar": "bar_buffer",
                    },
                    "next": "foobar",
                },
                "foobar": {
                    "type": "node",
                    "builder": "foobar",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "foobar");
    }

    #[test]
    /// join should be able to infer its output type when connected to terminate
    fn test_join_infer_from_terminate() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["foo", "bar"],
                },
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": "foo_buffer",
                },
                "foo_buffer": {
                    "type": "buffer",
                },
                "bar": {
                    "type": "node",
                    "builder": "bar",
                    "next": "bar_buffer",
                },
                "bar_buffer": {
                    "type": "buffer",
                },
                "join": {
                    "type": "join",
                    "buffers": {
                        "foo": "foo_buffer",
                        "bar": "bar_buffer",
                    },
                    "next": "fork_clone2",
                },
                "fork_clone2": {
                    "type": "fork_clone",
                    "next": [{ "builtin": "terminate" }],
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        let expectation = serde_json::Value::Object(
            serde_json::Map::from_iter([
                ("bar".to_string(), serde_json::Value::String("bar".to_string())),
                ("foo".to_string(), serde_json::Value::String("foo".to_string())),
            ])
        );
        assert_eq!(result, expectation);
    }

    #[test]
    fn test_join_buffer_array() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["foo", "bar"],
                },
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": "foo_buffer",
                },
                "foo_buffer": {
                    "type": "buffer",
                },
                "bar": {
                    "type": "node",
                    "builder": "bar",
                    "next": "bar_buffer",
                },
                "bar_buffer": {
                    "type": "buffer",
                },
                "join": {
                    "type": "join",
                    "buffers": ["foo_buffer", "bar_buffer"],
                    "target_node": "foobar_array",
                    "next": "foobar_array",
                },
                "foobar_array": {
                    "type": "node",
                    "builder": "foobar_array",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, "foobar");
    }

    #[test]
    fn test_empty_join() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "foo",
            "ops": {
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": { "builtin": "terminate" },
                },
                "join": {
                    "type": "join",
                    "buffers": [],
                    "target_node": "foobar",
                    "next": "foobar",
                },
                "foobar": {
                    "type": "node",
                    "builder": "foobar",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::EmptyJoin));
    }

    #[test]
    fn test_serialized_join() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["foo", "bar"],
                },
                "foo": {
                    "type": "node",
                    "builder": "foo",
                    "next": "foo_buffer",
                },
                "foo_buffer": {
                    "type": "buffer",
                    "serialize": true,
                },
                "bar": {
                    "type": "node",
                    "builder": "bar",
                    "next": "bar_buffer",
                },
                "bar_buffer": {
                    "type": "buffer",
                    "serialize": true,
                },
                "serialized_join": {
                    "type": "serialized_join",
                    "buffers": {
                        "foo": "foo_buffer",
                        "bar": "bar_buffer",
                    },
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result["foo"], "foo");
        assert_eq!(result["bar"], "bar");
    }

    #[test]
    fn test_serialized_join_with_unserialized_buffers() {
        let mut fixture = DiagramTestFixture::new();
        register_join_nodes(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["create_foobar_1", "create_foobar_2"],
                },
                "create_foobar_1": {
                    "type": "node",
                    "builder": "create_foobar",
                    "config": {
                        "foo": "foo_1",
                        "bar": "bar_1",
                    },
                    "next": "foobar_buffer_1",
                },
                "create_foobar_2": {
                    "type": "node",
                    "builder": "create_foobar",
                    "config": {
                        "foo": "foo_2",
                        "bar": "bar_2",
                    },
                    "next": "foobar_buffer_2",
                },
                "foobar_buffer_1": {
                    "type": "buffer",
                },
                "foobar_buffer_2": {
                    "type": "buffer",
                },
                "serialized_join": {
                    "type": "serialized_join",
                    "buffers": {
                        "foobar_1": "foobar_buffer_1",
                        "foobar_2": "foobar_buffer_2",
                    },
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        let object = result.as_object().unwrap();
        assert_eq!(object["foobar_1"].as_object().unwrap()["foo"], "foo_1");
        assert_eq!(object["foobar_1"].as_object().unwrap()["bar"], "bar_1");
        assert_eq!(object["foobar_2"].as_object().unwrap()["foo"], "foo_2");
        assert_eq!(object["foobar_2"].as_object().unwrap()["bar"], "bar_2");
    }
}
