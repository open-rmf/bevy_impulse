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

use super::{
    BufferSelection, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    JsonMessage, NextOperation, OperationName,
};
use crate::{default_as_false, is_default, is_false, BufferIdentifier};

/// Wait for exactly one item to be available in each buffer listed in
/// `buffers`, then join each of those items into a single output message
/// that gets sent to `next`.
///
/// If the `next` operation is not a `node` type (e.g. `fork_clone`) then
/// you must specify a `target_node` so that the diagram knows what data
/// structure to join the values into.
///
/// The output message type must be registered as joinable at compile time.
/// If you want to join into a dynamic data structure then you should use
/// [`DiagramOperation::SerializedJoin`] instead.
///
/// # Examples
/// ```
/// # bevy_impulse::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "begin_measuring",
///     "ops": {
///         "begin_measuring": {
///             "type": "fork_clone",
///             "next": ["localize", "imu"]
///         },
///         "localize": {
///             "type": "node",
///             "builder": "localize",
///             "next": "estimated_position"
///         },
///         "imu": {
///             "type": "node",
///             "builder": "imu",
///             "config": "velocity",
///             "next": "estimated_velocity"
///         },
///         "estimated_position": { "type": "buffer" },
///         "estimated_velocity": { "type": "buffer" },
///         "gather_state": {
///             "type": "join",
///             "buffers": {
///                 "position": "estimate_position",
///                 "velocity": "estimate_velocity"
///             },
///             "next": "report_state"
///         },
///         "report_state": {
///             "type": "node",
///             "builder": "publish_state",
///             "next": { "builtin": "terminate" }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinSchema {
    /// The operation that the joined value will be passed to.
    pub next: NextOperation,

    /// Map of buffer keys and buffers.
    pub buffers: BufferSelection,

    /// List of the keys in the `buffers` dictionary whose value should be cloned
    /// instead of removed from the buffer (pulled) when the join occurs. Cloning
    /// the value will leave the buffer unchanged after the join operation takes
    /// place.
    #[serde(default, skip_serializing_if = "is_default")]
    pub clone: Vec<BufferIdentifier<'static>>,

    /// Whether or not to automatically serialize the inputs into a single JsonMessage.
    /// This will only work if all input types are serializable, otherwise you will
    /// get a [`DiagramError`][super::DiagramError].
    #[serde(default = "default_as_false", skip_serializing_if = "is_false")]
    pub serialize: bool,
}

impl BuildDiagramOperation for JoinSchema {
    fn build_diagram_operation(
        &self,
        _: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let mut buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        for to_clone in &self.clone {
            let Some(buffer) = buffer_map.get_mut(to_clone) else {
                return Err(DiagramErrorCode::UnknownJoinField {
                    unknown: to_clone.clone(),
                    available: buffer_map.keys().cloned().collect(),
                });
            };

            *buffer = (*buffer)
                .join_by_cloning()
                .ok_or_else(|| DiagramErrorCode::NotCloneable(buffer.message_type()))?;
        }

        if self.serialize {
            let output = ctx.builder.try_join::<JsonMessage>(&buffer_map)?.output();
            ctx.add_output_into_target(&self.next, output.into());
        } else {
            let Some(target_type) = ctx.infer_input_type_into_target(&self.next)? else {
                return Ok(BuildStatus::defer(
                    "waiting to find out target message type",
                ));
            };

            let output = ctx
                .registry
                .messages
                .join(&target_type, &buffer_map, ctx.builder)?;
            ctx.add_output_into_target(&self.next, output);
        }
        Ok(BuildStatus::Finished)
    }
}

/// Same as [`DiagramOperation::Join`] but all input messages must be
/// serializable, and the output message will always be [`serde_json::Value`].
///
/// If you use an array for `buffers` then the output message will be a
/// [`serde_json::Value::Array`]. If you use a map for `buffers` then the
/// output message will be a [`serde_json::Value::Object`].
///
/// Unlike [`DiagramOperation::Join`], the `target_node` property does not
/// exist for this schema.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SerializedJoinSchema {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferSelection,
}

impl BuildDiagramOperation for SerializedJoinSchema {
    fn build_diagram_operation(
        &self,
        _: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let buffer_map = match ctx.create_buffer_map(&self.buffers) {
            Ok(buffer_map) => buffer_map,
            Err(reason) => return Ok(BuildStatus::defer(reason)),
        };

        let output = ctx.builder.try_join::<JsonMessage>(&buffer_map)?.output();
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
        diagram::testing::DiagramTestFixture, Diagram, DiagramElementRegistry, DiagramErrorCode,
        NodeBuilderOptions,
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
        let expectation = serde_json::Value::Object(serde_json::Map::from_iter([
            (
                "bar".to_string(),
                serde_json::Value::String("bar".to_string()),
            ),
            (
                "foo".to_string(),
                serde_json::Value::String("foo".to_string()),
            ),
        ]));
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
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

        let result: JsonMessage = fixture.spawn_and_run(&diagram, JsonMessage::Null).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        let object = result.as_object().unwrap();
        assert_eq!(object["foobar_1"].as_object().unwrap()["foo"], "foo_1");
        assert_eq!(object["foobar_1"].as_object().unwrap()["bar"], "bar_1");
        assert_eq!(object["foobar_2"].as_object().unwrap()["foo"], "foo_2");
        assert_eq!(object["foobar_2"].as_object().unwrap()["bar"], "bar_2");
    }

    #[test]
    fn test_diagram_join_by_clone() {
        let mut fixture = DiagramTestFixture::new();
        fixture
            .registry
            .register_node_builder(NodeBuilderOptions::new("check"), |builder, config: i64| {
                builder.create_map_block(move |joined: JoinByCloneTest| {
                    if joined.count < config {
                        Err(joined.count + 1)
                    } else {
                        Ok(joined)
                    }
                })
            })
            .with_join()
            .with_result();

        fixture
            .registry
            .register_message::<(String, i64)>()
            .with_unzip();

        // The diagram has a loop with a join of two buffers. One of the joined
        // bufffers gets cloned each time while the other gets pulled. We keep
        // refreshing the pulled buffer until its value reaches 10. We expect
        // the cloned value to keep being available over and over without putting
        // any new value inside of it because we are cloning from it instead of
        // pulling.
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "unzip",
            "ops": {
                "unzip": {
                    "type": "unzip",
                    "next": [
                        "message_buffer",
                        "count_buffer"
                    ]
                },
                "message_buffer": { "type": "buffer" },
                "count_buffer": { "type": "buffer" },
                "join": {
                    "type": "join",
                    "buffers": {
                        "count": "count_buffer",
                        "message": "message_buffer"
                    },
                    "clone": [ "message" ],
                    "next": "check"
                },
                "check": {
                    "type": "node",
                    "builder": "check",
                    "config": 10,
                    "next": "fork_result"
                },
                "fork_result": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": "count_buffer"
                }
            }
        }))
        .unwrap();

        let input = (String::from("hello"), 0_i64);
        let result: JoinByCloneTest = fixture.spawn_and_run(&diagram, input).unwrap();
        assert_eq!(result.count, 10);
        assert_eq!(result.message, "hello");
    }

    #[derive(Joined, Clone, Serialize, Deserialize, JsonSchema)]
    struct JoinByCloneTest {
        count: i64,
        message: String,
    }
}
