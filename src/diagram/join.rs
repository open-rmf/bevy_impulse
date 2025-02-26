use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{unknown_diagram_error, AnyBuffer, AnyMessageBox, BufferIdentifier, Builder};

use super::{
    buffer::{get_node_request_type, BufferInputs},
    workflow_builder::{Edge, EdgeBuilder, Vertex},
    Diagram, DiagramElementRegistry, DiagramErrorCode, NextOperation, OperationId,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this join is for. The id must be a `node` operation.
    pub(super) target_node: OperationId,
}

impl JoinOp {
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
        registry: &DiagramElementRegistry,
        buffers: &HashMap<&OperationId, AnyBuffer>,
        diagram: &Diagram,
    ) -> Result<bool, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };
        let target_type = get_node_request_type(&self.target_node, diagram, registry)?;
        let output = registry.messages.join(builder, &buffers, target_type)?;

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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SerializedJoinOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,
}

impl SerializedJoinOp {
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
        buffers: &HashMap<&OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode> {
        if self.buffers.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }

        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };
        let buffer_inputs = self.buffers.clone();
        let output = builder
            .try_join::<HashMap<BufferIdentifier<'static>, AnyMessageBox>>(&buffers)?
            .map_block(
                move |joined| -> Result<serde_json::Value, DiagramErrorCode> {
                    if joined.is_empty() {
                        return Ok(serde_json::Value::Null);
                    }

                    match &buffer_inputs {
                        BufferInputs::Dict(_) => {
                            let values = joined
                                .iter()
                                .filter_map(|(k, any_msg)| match k {
                                    BufferIdentifier::Name(k) => {
                                        match any_msg
                                            .downcast_ref::<serde_json::Value>()
                                            .ok_or(DiagramErrorCode::NotSerializable)
                                        {
                                            Ok(value) => Some(Ok((k, value))),
                                            Err(err) => Some(Err(err)),
                                        }
                                    }
                                    _ => None,
                                })
                                .collect::<Result<HashMap<_, _>, DiagramErrorCode>>()?;
                            Ok(serde_json::to_value(values).unwrap())
                        }
                        BufferInputs::Array(arr) => {
                            let values = (0..arr.len())
                                .map(|i| {
                                    let value = joined.get(&BufferIdentifier::Index(i)).unwrap();
                                    value
                                        .downcast_ref::<serde_json::Value>()
                                        .ok_or(DiagramErrorCode::NotSerializable)
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            Ok(serde_json::to_value(values).unwrap())
                        }
                    }
                },
            )
            .cancel_on_err()
            .output()
            .into();

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

/// The resulting type of a `join` operation. Nodes receiving a join output must have request
/// of this type. Note that the join output is NOT serializable. If you would like to serialize it,
/// convert it to a `Vec` first.
pub type JoinOutput<T> = SmallVec<[T; 4]>;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bevy_impulse_derive::Joined;
    use serde_json::json;
    use test_log::test;

    use super::*;
    use crate::{
        diagram::testing::DiagramTestFixture, Cancellation, CancellationCause, Diagram,
        DiagramErrorCode, FilteredErr, NodeBuilderOptions,
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

    fn foobar(foobar: FooBar) -> String {
        format!("{}{}", foobar.foo, foobar.bar)
    }

    fn foobar_array(foobar: Vec<String>) -> String {
        format!("{}{}", foobar[0], foobar[1])
    }

    fn register_join_nodes(registry: &mut DiagramElementRegistry) {
        registry
            .register_node_builder(
                NodeBuilderOptions::new("foo", "json", "String"),
                |builder, _config: ()| builder.create_map_block(foo),
            )
            .unwrap();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("bar", "json", "String"),
                |builder, _config: ()| builder.create_map_block(bar),
            )
            .unwrap();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("foobar", "foobar", "String"),
                |builder, _config: ()| builder.create_map_block(foobar),
            )
            .unwrap()
            .with_join()
            .unwrap();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("foobar_array", "Vec<String>", "String"),
                |builder, _config: ()| builder.create_map_block(foobar_array),
            )
            .unwrap()
            .with_join()
            .unwrap();
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
        assert_eq!(result, "foobar");
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

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
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
            .unwrap_err();
        let cause = result.downcast::<Cancellation>().unwrap().cause;
        let filtered = match cause.as_ref() {
            CancellationCause::Filtered(filtered) => filtered,
            _ => panic!("expected filtered"),
        };
        assert!(matches!(
            filtered
                .reason
                .as_ref()
                .unwrap()
                .downcast_ref::<FilteredErr<DiagramErrorCode>>()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<DiagramErrorCode>()
                .unwrap(),
            DiagramErrorCode::NotSerializable
        ));
    }
}
